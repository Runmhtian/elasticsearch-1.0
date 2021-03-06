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

package org.elasticsearch.discovery.zen.ping.unicast;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.discovery.zen.ping.ZenPing.PingResponse.readPingResponse;

/**
 *  单播
 *  这种机制  需要配置文件中配置host列表  依赖transport模块来发送request  也就是netty
 */
public class UnicastZenPing extends AbstractLifecycleComponent<ZenPing> implements ZenPing {

    public static final int LIMIT_PORTS_COUNT = 1;

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterName clusterName;
    private final Version version;

    private final int concurrentConnects;

    private final DiscoveryNode[] nodes;

    private volatile DiscoveryNodesProvider nodesProvider;

    private final AtomicInteger pingIdGenerator = new AtomicInteger();

    private final Map<Integer, ConcurrentMap<DiscoveryNode, PingResponse>> receivedResponses = newConcurrentMap();

    // a list of temporal responses a node will return for a request (holds requests from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue();

    private final CopyOnWriteArrayList<UnicastHostsProvider> hostsProviders = new CopyOnWriteArrayList<UnicastHostsProvider>();

    public UnicastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName, Version version, @Nullable Set<UnicastHostsProvider> unicastHostsProviders) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;
        this.version = version;

        if (unicastHostsProviders != null) {
            for (UnicastHostsProvider unicastHostsProvider : unicastHostsProviders) {
                addHostsProvider(unicastHostsProvider);
            }
        }

        this.concurrentConnects = componentSettings.getAsInt("concurrent_connects", 10);
        String[] hostArr = componentSettings.getAsArray("hosts");
        // trim the hosts
        for (int i = 0; i < hostArr.length; i++) {
            hostArr[i] = hostArr[i].trim();
        }
        List<String> hosts = Lists.newArrayList(hostArr);
        logger.debug("using initial hosts {}, with concurrent_connects [{}]", hosts, concurrentConnects);

        List<DiscoveryNode> nodes = Lists.newArrayList();
        int idCounter = 0;
        for (String host : hosts) {
            try {
                TransportAddress[] addresses = transportService.addressesFromString(host);
                // we only limit to 1 addresses, makes no sense to ping 100 ports
                for (int i = 0; (i < addresses.length && i < LIMIT_PORTS_COUNT); i++) {
                    nodes.add(new DiscoveryNode("#zen_unicast_" + (++idCounter) + "#", addresses[i], version));
                }
            } catch (Exception e) {
                throw new ElasticsearchIllegalArgumentException("Failed to resolve address for [" + host + "]", e);
            }
        }
        this.nodes = nodes.toArray(new DiscoveryNode[nodes.size()]);

        transportService.registerHandler(UnicastPingRequestHandler.ACTION, new UnicastPingRequestHandler());
    }


    /**
     * 单播 start不用初始化
     * @throws ElasticsearchException
     */
    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        transportService.removeHandler(UnicastPingRequestHandler.ACTION);
    }

    public void addHostsProvider(UnicastHostsProvider provider) {
        hostsProviders.add(provider);
    }

    public void removeHostsProvider(UnicastHostsProvider provider) {
        hostsProviders.remove(provider);
    }

    @Override
    public void setNodesProvider(DiscoveryNodesProvider nodesProvider) {
        this.nodesProvider = nodesProvider;
    }

    public PingResponse[] pingAndWait(TimeValue timeout) {
        final AtomicReference<PingResponse[]> response = new AtomicReference<PingResponse[]>();
        final CountDownLatch latch = new CountDownLatch(1);
        ping(new PingListener() {
            @Override
            public void onPing(PingResponse[] pings) {
                response.set(pings);
                latch.countDown();
            }
        }, timeout);
        try {
            latch.await();
            return response.get();
        } catch (InterruptedException e) {
            return null;
        }
    }

    /**
     *  在 timeout时间内 多次执行ping，共三次    拿到response  PingListener存储 response
     * @param listener
     * @param timeout
     * @throws ElasticsearchException
     */
    @Override
    public void ping(final PingListener listener, final TimeValue timeout) throws ElasticsearchException {
        final SendPingsHandler sendPingsHandler = new SendPingsHandler(pingIdGenerator.incrementAndGet());
        // receivedResponses 临时存放 response的地方， 多个方法间不用传递参数
        receivedResponses.put(sendPingsHandler.id(), ConcurrentCollections.<DiscoveryNode, PingResponse>newConcurrentMap());
        sendPings(timeout, null, sendPingsHandler);  //一上来就ping
        // 给定延迟有执行一次
        threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                try {
                    // 二分之一timeout后再执行一次
                    sendPings(timeout, null, sendPingsHandler);

                    threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.GENERIC, new Runnable() {
                        @Override
                        public void run() {
                            try {
                                //再过二分之一timeout后再执行一次
                                sendPings(timeout, TimeValue.timeValueMillis(timeout.millis() / 2), sendPingsHandler);
                                // 去除 并拿到responses
                                ConcurrentMap<DiscoveryNode, PingResponse> responses = receivedResponses.remove(sendPingsHandler.id());
                                sendPingsHandler.close();
                                for (DiscoveryNode node : sendPingsHandler.nodeToDisconnect) {
                                    logger.trace("[{}] disconnecting from {}", sendPingsHandler.id(), node);
                                    // 断开链接
                                    transportService.disconnectFromNode(node);
                                }
                                listener.onPing(responses.values().toArray(new PingResponse[responses.size()]));
                            } catch (EsRejectedExecutionException ex) {
                                logger.debug("Ping execution rejected", ex);
                            }
                        }
                    });
                } catch (EsRejectedExecutionException ex) {
                    logger.debug("Ping execution rejected", ex);
                }
            }
        });
    }

    class SendPingsHandler {
        private final int id;
        private volatile ExecutorService executor;
        private final Set<DiscoveryNode> nodeToDisconnect = ConcurrentCollections.newConcurrentSet();
        private volatile boolean closed;

        SendPingsHandler(int id) {
            this.id = id;
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed;
        }

        public Executor executor() {
            if (executor == null) {
                ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_connect]");
                executor = EsExecutors.newScaling(0, concurrentConnects, 60, TimeUnit.SECONDS, threadFactory);
            }
            return executor;
        }

        public void close() {
            closed = true;
            if (executor != null) {
                executor.shutdownNow();
                executor = null;
            }
        }
    }

    void sendPings(final TimeValue timeout, @Nullable TimeValue waitTime, final SendPingsHandler sendPingsHandler) {
        final UnicastPingRequest pingRequest = new UnicastPingRequest();
        pingRequest.id = sendPingsHandler.id();
        pingRequest.timeout = timeout;
        // 已经发现的node  也就是现有集群中的节点
        DiscoveryNodes discoNodes = nodesProvider.nodes();
        //target 是localNode
        pingRequest.pingResponse = new PingResponse(discoNodes.localNode(), discoNodes.masterNode(), clusterName);
        // 配置的所有node
        List<DiscoveryNode> nodesToPing = newArrayList(nodes);
        for (UnicastHostsProvider provider : hostsProviders) {
            nodesToPing.addAll(provider.buildDynamicNodes());
        }

        final CountDownLatch latch = new CountDownLatch(nodesToPing.size());
        for (final DiscoveryNode node : nodesToPing) {
            // make sure we are connected
            boolean nodeFoundByAddressX;
            DiscoveryNode nodeToSendX = discoNodes.findByAddress(node.address());
            if (nodeToSendX != null) {
                nodeFoundByAddressX = true;
            } else {
                nodeToSendX = node;
                nodeFoundByAddressX = false;
            }
            final DiscoveryNode nodeToSend = nodeToSendX;

            final boolean nodeFoundByAddress = nodeFoundByAddressX;
            //ping
            if (!transportService.nodeConnected(nodeToSend)) {  //没有连接
                if (sendPingsHandler.isClosed()) {
                    return;
                }
                sendPingsHandler.nodeToDisconnect.add(nodeToSend);
                // fork the connection to another thread
                sendPingsHandler.executor().execute(new Runnable() {
                    @Override
                    public void run() {
                        if (sendPingsHandler.isClosed()) {
                            return;
                        }
                        boolean success = false;
                        try {
                            //尝试连接node
                            // connect to the node, see if we manage to do it, if not, bail
                            if (!nodeFoundByAddress) {
                                logger.trace("[{}] connecting (light) to {}", sendPingsHandler.id(), nodeToSend);
                                transportService.connectToNodeLight(nodeToSend);// light connect 一个node一个channals
                            } else {
                                logger.trace("[{}] connecting to {}", sendPingsHandler.id(), nodeToSend);
                                transportService.connectToNode(nodeToSend);  //连接node 已有会直接返回
                            }
                            logger.trace("[{}] connected to {}", sendPingsHandler.id(), node);
                            if (receivedResponses.containsKey(sendPingsHandler.id())) {
                                // we are connected and still in progress, send the ping request
                                sendPingRequestToNode(sendPingsHandler.id(), timeout, pingRequest, latch, node, nodeToSend);
                            } else {
                                // connect took too long, just log it and bail
                                latch.countDown();
                                logger.trace("[{}] connect to {} was too long outside of ping window, bailing", sendPingsHandler.id(), node);
                            }
                            success = true;
                        } catch (ConnectTransportException e) {
                            // can't connect to the node - this is a more common path!
                            logger.trace("[{}] failed to connect to {}", e, sendPingsHandler.id(), nodeToSend);
                        } catch (Throwable e) {
                            logger.warn("[{}] failed send ping to {}", e, sendPingsHandler.id(), nodeToSend);
                        } finally {
                            if (!success) {
                                latch.countDown();
                            }
                        }
                    }
                });
            } else {
//                sendRequest
                sendPingRequestToNode(sendPingsHandler.id(), timeout, pingRequest, latch, node, nodeToSend);
            }
        }
        if (waitTime != null) {
            try {
                latch.await(waitTime.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void sendPingRequestToNode(final int id, TimeValue timeout, UnicastPingRequest pingRequest, final CountDownLatch latch, final DiscoveryNode node, final DiscoveryNode nodeToSend) {
        logger.trace("[{}] sending to {}", id, nodeToSend);
        //发送ping求情
        transportService.sendRequest(nodeToSend, UnicastPingRequestHandler.ACTION, pingRequest, TransportRequestOptions.options().withTimeout((long) (timeout.millis() * 1.25)), new BaseTransportResponseHandler<UnicastPingResponse>() {

            @Override
            public UnicastPingResponse newInstance() {
                return new UnicastPingResponse();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

//            合并response 到receivedResponses中
            @Override
            public void handleResponse(UnicastPingResponse response) {
                logger.trace("[{}] received response from {}: {}", id, nodeToSend, Arrays.toString(response.pingResponses));
                try {
                    DiscoveryNodes discoveryNodes = nodesProvider.nodes();
                    for (PingResponse pingResponse : response.pingResponses) {
                        if (pingResponse.target().id().equals(discoveryNodes.localNodeId())) {
                            // 这里自己发的请求响应 跳过？
                            // that's us, ignore
                            continue;
                        }
                        if (!pingResponse.clusterName().equals(clusterName)) {
                            // not part of the cluster
                            logger.debug("[{}] filtering out response from {}, not same cluster_name [{}]", id, pingResponse.target(), pingResponse.clusterName().value());
                            continue;
                        }
                        // Response 都存入了receivedResponses中
                        ConcurrentMap<DiscoveryNode, PingResponse> responses = receivedResponses.get(response.id);
                        if (responses == null) {
                            logger.warn("received ping response {} with no matching id [{}]", pingResponse, response.id);
                        } else {
                            // 合并repsonse
                            PingResponse existingResponse = responses.get(pingResponse.target());
                            if (existingResponse == null) {
                                responses.put(pingResponse.target(), pingResponse);
                            } else {
                                // try and merge the best ping response for it, i.e. if the new one
                                // doesn't have the master node set, and the existing one does, then
                                // the existing one is better, so we keep it
                                if (pingResponse.master() != null) {
                                    responses.put(pingResponse.target(), pingResponse);
                                }
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                latch.countDown();
                if (exp instanceof ConnectTransportException) {
                    // ok, not connected...
                    logger.trace("failed to connect to {}", exp, nodeToSend);
                } else {
                    logger.warn("failed to send ping to [{}]", exp, node);
                }
            }
        });
    }

    /**
     * 返回的UnicastPingResponse 包含三部分pingResponse
     * 1.request中的pingResponse
     * 2.temporalResponses集合中的pingResponse，2*timeout时间会过期
     * 3.localNode生成的pingResponse
     * @param request
     * @return
     */
    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) {
        if (lifecycle.stoppedOrClosed()) {
            throw new ElasticsearchIllegalStateException("received ping request while stopped/closed");
        }
        temporalResponses.add(request.pingResponse);
        // 2个timeout时间后 删除pingResponse
        threadPool.schedule(TimeValue.timeValueMillis(request.timeout.millis() * 2), ThreadPool.Names.SAME, new Runnable() {
            @Override
            public void run() {
                temporalResponses.remove(request.pingResponse);
            }
        });

        List<PingResponse> pingResponses = newArrayList(temporalResponses);
        DiscoveryNodes discoNodes = nodesProvider.nodes();
        // 添加response 这个是自己节点的信息
        pingResponses.add(new PingResponse(discoNodes.localNode(), discoNodes.masterNode(), clusterName));


        UnicastPingResponse unicastPingResponse = new UnicastPingResponse();
        unicastPingResponse.id = request.id;
        unicastPingResponse.pingResponses = pingResponses.toArray(new PingResponse[pingResponses.size()]);

        return unicastPingResponse;
    }

    /**
     * 处理接收到的  UnicastPingRequest
     */
    class UnicastPingRequestHandler extends BaseTransportRequestHandler<UnicastPingRequest> {

        static final String ACTION = "discovery/zen/unicast";

        @Override
        public UnicastPingRequest newInstance() {
            return new UnicastPingRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(UnicastPingRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(handlePingRequest(request));
        }
    }

    static class UnicastPingRequest extends TransportRequest {

        int id;

        TimeValue timeout;

        PingResponse pingResponse;

        UnicastPingRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readInt();
            timeout = readTimeValue(in);
            pingResponse = readPingResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            timeout.writeTo(out);
            pingResponse.writeTo(out);
        }
    }

    static class UnicastPingResponse extends TransportResponse {

        int id;

        PingResponse[] pingResponses;

        UnicastPingResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readInt();
            pingResponses = new PingResponse[in.readVInt()];
            for (int i = 0; i < pingResponses.length; i++) {
                pingResponses[i] = readPingResponse(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeVInt(pingResponses.length);
            for (PingResponse pingResponse : pingResponses) {
                pingResponse.writeTo(out);
            }
        }
    }
}
