/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeOperationAction<Request extends MasterNodeOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    final String transportAction;
    final String executor;

    protected TransportMasterNodeOperationAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, threadPool);
        this.transportService = transportService;
        this.clusterService = clusterService;

        this.transportAction = transportAction();
        this.executor = executor();

        transportService.registerHandler(transportAction, new TransportHandler());  // 这里注册了  handler
    }

    protected abstract String transportAction();

    protected abstract String executor();

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws ElasticsearchException;

    protected boolean localExecute(Request request) {
        return false;
    }

    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    protected void processBeforeDelegationToMaster(Request request, ClusterState state) {

    }

    @Override
    public void execute(Request request, ActionListener<Response> listener) {
        // since the callback is async, we typically can get called from within an event in the cluster service
        // or something similar, so make sure we are threaded so we won't block it.
        request.listenerThreaded(true);
        super.execute(request, listener);
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        innerExecute(request, listener, false);
    }
    //需要mater节点执行的request，调用此方法,  子类继承
    private void innerExecute(final Request request, final ActionListener<Response> listener, final boolean retrying) {
        final ClusterState clusterState = clusterService.state();// 获取集群信息
        final DiscoveryNodes nodes = clusterState.nodes();  // 获取到所有node信息
        if (nodes.localNodeMaster() || localExecute(request)) { //判断本节点是否是master节点，或者指定为本地执行模式
            // check for block, if blocked, retry, else, execute locally
            final ClusterBlockException blockException = checkBlock(request, clusterState);  //null
            if (blockException != null) {
                if (!blockException.retryable()) {
                    listener.onFailure(blockException);
                    return;
                }
                clusterService.add(request.masterNodeTimeout(), new TimeoutClusterStateListener() {
                    @Override
                    public void postAdded() {
                        ClusterBlockException blockException = checkBlock(request, clusterService.state());
                        if (blockException == null || !blockException.retryable()) {
                            clusterService.remove(this);
                            innerExecute(request, listener, false);
                        }
                    }

                    @Override
                    public void onClose() {
                        clusterService.remove(this);
                        listener.onFailure(blockException);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        clusterService.remove(this);
                        listener.onFailure(blockException);
                    }

                    @Override
                    public void clusterChanged(ClusterChangedEvent event) {
                        ClusterBlockException blockException = checkBlock(request, event.state());
                        if (blockException == null || !blockException.retryable()) {
                            clusterService.remove(this);
                            innerExecute(request, listener, false);
                        }
                    }
                });
            } else {
                //使用SAME来执行masterOperation方法   同步运行
                try {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                //调用子类实现方法
                                masterOperation(request, clusterService.state(), listener);
                            } catch (Throwable e) {
                                listener.onFailure(e);
                            }
                        }
                    });
                } catch (Throwable t) {
                    listener.onFailure(t);
                }
            }
        } else {// 若不是master节点

            if (nodes.masterNode() == null) { //判断是否有mater节点
                if (retrying) { //没有重试，则返回response
                    listener.onFailure(new MasterNotDiscoveredException());
                } else {
                    // 30s timeout后再次尝试执行，并添加到clusterStateListeners ，clusterChange时调用listener的clusterChanged方法
                    // 难道会一直等待执行成功，或者调用postAdded 出现异常 EsRejectedExecutionException？？
                    clusterService.add(request.masterNodeTimeout(), new TimeoutClusterStateListener() {
                        @Override
                        public void postAdded() {
                            ClusterState clusterStateV2 = clusterService.state();
                            if (clusterStateV2.nodes().masterNodeId() != null) {
                                // now we have a master, try and execute it...
                                clusterService.remove(this);//从clusterService 去除掉此listener
                                innerExecute(request, listener, true);//再次调用
                            }
                        }

                        @Override
                        public void onClose() {
                            clusterService.remove(this);
                            listener.onFailure(new NodeClosedException(clusterService.localNode()));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            clusterService.remove(this);
                            listener.onFailure(new MasterNotDiscoveredException("waited for [" + timeout + "]"));
                        }

                        @Override
                        public void clusterChanged(ClusterChangedEvent event) {
                            if (event.nodesDelta().masterNodeChanged()) {//master 改变时调用
                                clusterService.remove(this);
                                innerExecute(request, listener, true);
                            }
                        }
                    });
                }
                return;
            }
            //不是master节点，并且master节点不为null，则向下执行
            processBeforeDelegationToMaster(request, clusterState); //没有操作
            //转发请求到 master节点。  同样执行innerExecute函数。
            transportService.sendRequest(nodes.masterNode(), transportAction, request, new BaseTransportResponseHandler<Response>() {
                @Override
                public Response newInstance() {
                    return newResponse();
                }

                //从这可以看到，master节点处理完之后，发送响应到此节点上，此节点再response客户端
                @Override
                public void handleResponse(Response response) {
                    listener.onResponse(response);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleException(final TransportException exp) {
                    if (exp.unwrapCause() instanceof ConnectTransportException) {
                        // we want to retry here a bit to see if a new master is elected
                        clusterService.add(request.masterNodeTimeout(), new TimeoutClusterStateListener() {
                            @Override
                            public void postAdded() {
                                ClusterState clusterStateV2 = clusterService.state();
                                if (!clusterState.nodes().masterNodeId().equals(clusterStateV2.nodes().masterNodeId())) {
                                    // master changes while adding the listener, try here
                                    clusterService.remove(this);
                                    innerExecute(request, listener, false);
                                }
                            }

                            @Override
                            public void onClose() {
                                clusterService.remove(this);
                                listener.onFailure(new NodeClosedException(clusterService.localNode()));
                            }

                            @Override
                            public void onTimeout(TimeValue timeout) {
                                clusterService.remove(this);
                                listener.onFailure(new MasterNotDiscoveredException());
                            }

                            @Override
                            public void clusterChanged(ClusterChangedEvent event) {
                                if (event.nodesDelta().masterNodeChanged()) {
                                    clusterService.remove(this);
                                    innerExecute(request, listener, false);
                                }
                            }
                        });
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }  // 使用的是same，请求不是交由线程池执行

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }
    }
}
