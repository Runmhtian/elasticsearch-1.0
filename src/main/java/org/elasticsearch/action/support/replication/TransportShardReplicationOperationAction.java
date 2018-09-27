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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;

/**
 */
public abstract class TransportShardReplicationOperationAction<Request extends ShardReplicationOperationRequest, ReplicaRequest extends ActionRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final ShardStateAction shardStateAction;
    protected final ReplicationType defaultReplicationType;
    protected final WriteConsistencyLevel defaultWriteConsistencyLevel;
    protected final TransportRequestOptions transportOptions;

    final String transportAction;
    final String transportReplicaAction;
    final String executor;
    final boolean checkWriteConsistency;

    protected TransportShardReplicationOperationAction(Settings settings, TransportService transportService,
                                                       ClusterService clusterService, IndicesService indicesService,
                                                       ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings, threadPool);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;

        this.transportAction = transportAction();
        this.transportReplicaAction = transportReplicaAction();
        this.executor = executor();
        this.checkWriteConsistency = checkWriteConsistency();

        transportService.registerHandler(transportAction, new OperationTransportHandler());  //index
        transportService.registerHandler(transportReplicaAction, new ReplicaOperationTransportHandler());// index/replica

        this.transportOptions = transportOptions();

        this.defaultReplicationType = ReplicationType.fromString(settings.get("action.replication_type", "sync"));
        this.defaultWriteConsistencyLevel = WriteConsistencyLevel.fromString(settings.get("action.write_consistency", "quorum"));
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected abstract Request newRequestInstance();

    protected abstract ReplicaRequest newReplicaRequestInstance();

    protected abstract Response newResponseInstance();

    protected abstract String transportAction();

    protected abstract String executor();

    protected abstract PrimaryResponse<Response, ReplicaRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest);

    protected abstract void shardOperationOnReplica(ReplicaOperationRequest shardRequest);

    /**
     * Called once replica operations have been dispatched on the
     */
    protected void postPrimaryOperation(Request request, PrimaryResponse<Response, ReplicaRequest> response) {

    }

    protected abstract ShardIterator shards(ClusterState clusterState, Request request) throws ElasticsearchException;

    protected abstract boolean checkWriteConsistency();

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    /**
     * Resolves the request, by default, simply setting the concrete index (if its aliased one). If the resolve
     * means a different execution, then return false here to indicate not to continue and execute this request.
     */
    protected boolean resolveRequest(ClusterState state, Request request, ActionListener<Response> listener) {
        request.index(state.metaData().concreteIndex(request.index()));
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should the operations be performed on the replicas as well. Defaults to <tt>false</tt> meaning operations
     * will be executed on the replica.
     */
    protected boolean ignoreReplicas() {
        return false;
    }

    private String transportReplicaAction() {
        return transportAction() + "/replica";
    }

    protected boolean retryPrimaryException(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    /**
     * Should an exception be ignored when the operation is performed on the replica.
     */
    boolean ignoreReplicaException(Throwable e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof ConnectTransportException) {
            return true;
        }
        // on version conflict or document missing, it means
        // that a news change has crept into the replica, and its fine
        if (cause instanceof VersionConflictEngineException) {
            return true;
        }
        // same here
        if (cause instanceof DocumentAlreadyExistsException) {
            return true;
        }
        return false;
    }

    class OperationTransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequestInstance();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Throwable e1) {
                        logger.warn("Failed to send response for " + transportAction, e1);
                    }
                }
            });
        }
    }

    class ReplicaOperationTransportHandler extends BaseTransportRequestHandler<ReplicaOperationRequest> {

        @Override
        public ReplicaOperationRequest newInstance() {
            return new ReplicaOperationRequest();
        }

        @Override
        public String executor() {
            return executor;
        }

        // we must never reject on because of thread pool capacity on replicas
        @Override
        public boolean isForceExecution() {
            return true;
        }

        @Override
        public void messageReceived(final ReplicaOperationRequest request, final TransportChannel channel) throws Exception {
            shardOperationOnReplica(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    protected class PrimaryOperationRequest implements Streamable {

        public int shardId;

        public Request request;

        public PrimaryOperationRequest() {
        }

        public PrimaryOperationRequest(int shardId, Request request) {
            this.shardId = shardId;
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            shardId = in.readVInt();
            request = newRequestInstance();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shardId);
            request.writeTo(out);
        }
    }

    protected class ReplicaOperationRequest extends TransportRequest {

        public int shardId;
        public ReplicaRequest request;

        public ReplicaOperationRequest() {
        }

        public ReplicaOperationRequest(int shardId, ReplicaRequest request) {
            super(request);
            this.shardId = shardId;
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = in.readVInt();
            request = newReplicaRequestInstance();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shardId);
            request.writeTo(out);
        }
    }

    protected class AsyncShardOperationAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterState clusterState;
        private volatile ShardIterator shardIt;
        private final AtomicBoolean primaryOperationStarted = new AtomicBoolean();
        private final ReplicationType replicationType;
        protected final long startTime = System.currentTimeMillis();

        AsyncShardOperationAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            if (request.replicationType() != ReplicationType.DEFAULT) {// 判断请求的复制类型
                replicationType = request.replicationType();
            } else {
                replicationType = defaultReplicationType; //使用配置文件中配置的type，默认是sync
            }
        }

        public void start() {
            start(false);
        }

        /**
         * Returns <tt>true</tt> if the action starting to be performed on the primary (or is done).
         */
        public boolean start(final boolean fromClusterEvent) throws ElasticsearchException {
            this.clusterState = clusterService.state();
            try {
                ClusterBlockException blockException = checkGlobalBlock(clusterState, request);//判断cluster是否 write block
                if (blockException != null) {  // 集群不允许写
                    if (blockException.retryable()) {
                        logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        retry(fromClusterEvent, blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                // check if we need to execute, and if not, return   检查request  索引或者别名转换
                if (!resolveRequest(clusterState, request, listener)) {
                    return true;
                }
                blockException = checkRequestBlock(clusterState, request);//判断index write block
                if (blockException != null) {
                    if (blockException.retryable()) {
                        logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        retry(fromClusterEvent, blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                /*
               根据请求 索引，文档id，routing，type，找到对应的  IndexShardRoutingTable （一个分片的所有实例  主 备）
               进而得到ShardIterator
                 */
                shardIt = shards(clusterState, request);
            } catch (Throwable e) {
                listener.onFailure(e);
                return true;
            }
            //没有找到对应的shard，可能分片正在恢复
            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                logger.trace("no shard instances known for shard [{}], scheduling a retry", shardIt.shardId());

                retry(fromClusterEvent, null);
                return false;
            }

            boolean foundPrimary = false;
            ShardRouting shardX;
            while ((shardX = shardIt.nextOrNull()) != null) {
                final ShardRouting shard = shardX;
                // we only deal with primary shardIt here...  找到主分片
                if (!shard.primary()) {
                    continue;
                }
                //分片不是active，或者节点不存在
                if (!shard.active() || !clusterState.nodes().nodeExists(shard.currentNodeId())) {
                    logger.trace("primary shard [{}] is not yet active or we do not know the node it is assigned to [{}], scheduling a retry.", shard.shardId(), shard.currentNodeId());
                    retry(fromClusterEvent, null);
                    return false;
                }

                // check here for consistency
                if (checkWriteConsistency) { // index true 若是需要检查写一致性   保证写的时候活跃的分片数大于一定的数目
                    WriteConsistencyLevel consistencyLevel = defaultWriteConsistencyLevel;   ///默认为quorum
                    if (request.consistencyLevel() != WriteConsistencyLevel.DEFAULT) {
                        consistencyLevel = request.consistencyLevel();
                    }
                    int requiredNumber = 1;  // 计算quorum需要的数目  默认为1 WriteConsistencyLevel为 one
                    if (consistencyLevel == WriteConsistencyLevel.QUORUM && shardIt.size() > 2) {
                        // only for more than 2 in the number of shardIt it makes sense, otherwise its 1 shard with 1 replica, quorum is 1 (which is what it is initialized to)
                        requiredNumber = (shardIt.size() / 2) + 1;
                    } else if (consistencyLevel == WriteConsistencyLevel.ALL) {
                        requiredNumber = shardIt.size();
                    }
//                    若是分片下 活跃的主备数 小于需求quorum的数目
                    if (shardIt.sizeActive() < requiredNumber) {
                        logger.trace("not enough active copies of shard [{}] to meet write consistency of [{}] (have {}, needed {}), scheduling a retry.",
                                shard.shardId(), consistencyLevel, shardIt.sizeActive(), requiredNumber);
                        retry(fromClusterEvent, null);
                        return false;
                    }
                }

                if (!primaryOperationStarted.compareAndSet(false, true)) {  //cas 置为true   只执行一次
                    return true;
                }

                foundPrimary = true;
                if (shard.currentNodeId().equals(clusterState.nodes().localNodeId())) {  //主分片是当前节点
                    try {
                        if (request.operationThreaded()) { // 请求是否需要 线程执行  true
                            request.beforeLocalFork();
                            threadPool.executor(executor).execute(new Runnable() {
                                @Override
                                public void run() {  //INDEX线程池
                                    try {
                                        performOnPrimary(shard.id(), shard, clusterState);
                                    } catch (Throwable t) {
                                        listener.onFailure(t);
                                    }
                                }
                            });
                        } else {
                            //在主分片上运行
                            performOnPrimary(shard.id(), shard, clusterState);
                        }
                    } catch (Throwable t) {
                        listener.onFailure(t);
                    }
                } else {
                    //否则的话 发请求到主分片所在节点，从OperationTransportHandler处开始执行  到performOnPrimary
                    DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                    transportService.sendRequest(node, transportAction, request, transportOptions, new BaseTransportResponseHandler<Response>() {

                        @Override
                        public Response newInstance() {
                            return newResponseInstance();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                            if (exp.unwrapCause() instanceof ConnectTransportException || exp.unwrapCause() instanceof NodeClosedException ||
                                    retryPrimaryException(exp)) {
                                primaryOperationStarted.set(false);
                                // we already marked it as started when we executed it (removed the listener) so pass false
                                // to re-add to the cluster listener
                                logger.trace("received an error from node the primary was assigned to ({}), scheduling a retry", exp.getMessage());
                                retry(false, null);
                            } else {
                                listener.onFailure(exp);
                            }
                        }
                    });
                }
                break;
            }
            // we won't find a primary if there are no shards in the shard iterator, retry...
            if (!foundPrimary) {
                logger.trace("couldn't find a eligible primary shard, scheduling for retry.");
                retry(fromClusterEvent, null);
                return false;
            }
            return true;
        }

        void retry(boolean fromClusterEvent, @Nullable final Throwable failure) {
            if (fromClusterEvent) {
                logger.trace("retry scheduling ignored as it as we already have a listener in place");
                return;
            }

            // make it threaded operation so we fork on the discovery listener thread
            request.beforeLocalFork();
            request.operationThreaded(true);

            TimeValue timeout = new TimeValue(request.timeout().millis() - (System.currentTimeMillis() - startTime));
            if (timeout.millis() <= 0) {
                raiseTimeoutFailure(timeout, failure);
                return;
            }

            clusterService.add(timeout, new TimeoutClusterStateListener() {
                @Override
                public void postAdded() {
                    // check if state version changed while we were adding this listener
                    long sampledVersion = clusterState.version();
                    long currentVersion = clusterService.state().version();
                    if (sampledVersion != currentVersion) {
                        logger.trace("state change while we were trying to add listener, trying to start again, sampled_version [{}], current_version [{}]", sampledVersion, currentVersion);
                        if (start(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }
                }

                @Override
                public void onClose() {
                    clusterService.remove(this);
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    logger.trace("cluster changed (version {}), trying to start again", event.state().version());
                    if (start(true)) {
                        // if we managed to start and perform the operation on the primary, we can remove this listener
                        clusterService.remove(this);
                    }
                }

                @Override
                public void onTimeout(TimeValue timeValue) {
                    // just to be on the safe side, see if we can start it now?
                    if (start(true)) {
                        clusterService.remove(this);
                        return;
                    }
                    clusterService.remove(this);
                    raiseTimeoutFailure(timeValue, failure);
                }
            });
        }

        void raiseTimeoutFailure(TimeValue timeout, @Nullable Throwable failure) {
            if (failure == null) {
                if (shardIt == null) {
                    failure = new UnavailableShardsException(null, "no available shards: Timeout waiting for [" + timeout + "], request: " + request.toString());
                } else {
                    failure = new UnavailableShardsException(shardIt.shardId(), "[" + shardIt.size() + "] shardIt, [" + shardIt.sizeActive() + "] active : Timeout waiting for [" + timeout + "], request: " + request.toString());
                }
            }
            listener.onFailure(failure);
        }

        void performOnPrimary(int primaryShardId, final ShardRouting shard, ClusterState clusterState) {
            try {
                //在主分片上进行  得到主分片的响应
                PrimaryResponse<Response, ReplicaRequest> response = shardOperationOnPrimary(clusterState, new PrimaryOperationRequest(primaryShardId, request));
                //在副本分片上进行
                performReplicas(response);
            } catch (Throwable e) {
                // shard has not been allocated yet, retry it here
                if (retryPrimaryException(e)) {
                    primaryOperationStarted.set(false);
                    logger.trace("had an error while performing operation on primary ({}), scheduling a retry.", e.getMessage());
                    retry(false, e);
                    return;
                }
                if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(shard.shortSummary() + ": Failed to execute [" + request + "]", e);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", e);
                    }
                }
                listener.onFailure(e);
            }
        }

        void performReplicas(final PrimaryResponse<Response, ReplicaRequest> response) {
            if (ignoreReplicas()) {  //判断此action是否  忽略 副本
                postPrimaryOperation(request, response);
                listener.onResponse(response.response());
                return;
            }

            ShardRouting shard;

            // we double check on the state, if it got changed we need to make sure we take the latest one cause
            // maybe a replica shard started its recovery process and we need to apply it there...

            // we also need to make sure if the new state has a new primary shard (that we indexed to before) started
            // and assigned to another node (while the indexing happened). In that case, we want to apply it on the
            // new primary shard as well...
            ClusterState newState = clusterService.state();
            ShardRouting newPrimaryShard = null;
            if (clusterState != newState) {  //集群状态改变
                shardIt.reset();
                ShardRouting originalPrimaryShard = null;
                while ((shard = shardIt.nextOrNull()) != null) {  //原状态的主分片
                    if (shard.primary()) {
                        originalPrimaryShard = shard;
                        break;
                    }
                }
                if (originalPrimaryShard == null || !originalPrimaryShard.active()) {
                    throw new ElasticsearchIllegalStateException("unexpected state, failed to find primary shard on an index operation that succeeded");
                }

                clusterState = newState;
                shardIt = shards(newState, request); //现在状态的分片迭代
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        if (originalPrimaryShard.currentNodeId().equals(shard.currentNodeId())) { //原来主分片的节点等于现在主分片的节点  也就是主分片没有移动
                            newPrimaryShard = null;
                        } else {
                            newPrimaryShard = shard;
                        }
                        break;
                    }
                }
            }

            // initialize the counter
            int replicaCounter = shardIt.assignedReplicasIncludingRelocating();

            if (newPrimaryShard != null) {
                replicaCounter++;
            }

            if (replicaCounter == 0) {
                postPrimaryOperation(request, response);
                listener.onResponse(response.response());
                return;
            }

            if (replicationType == ReplicationType.ASYNC) {  //异步直接响应  并向下执行
                postPrimaryOperation(request, response);
                // async replication, notify the listener
                listener.onResponse(response.response());
                // now, trick the counter so it won't decrease to 0 and notify the listeners
                replicaCounter = Integer.MIN_VALUE;
            }

            // we add one to the replica count to do the postPrimaryOperation
            replicaCounter++;
            AtomicInteger counter = new AtomicInteger(replicaCounter); //用来 记录副本写入成功


            IndexMetaData indexMetaData = clusterState.metaData().index(request.index());

            if (newPrimaryShard != null) { //集群主节点改变
                performOnReplica(response, counter, newPrimaryShard, newPrimaryShard.currentNodeId(), indexMetaData);
            }

            shardIt.reset(); // reset the iterator
            while ((shard = shardIt.nextOrNull()) != null) {
                // if its unassigned, nothing to do here...
                if (shard.unassigned()) {
                    continue;
                }

                // if the shard is primary and relocating, add one to the counter since we perform it on the replica as well
                // (and we already did it on the primary)
                boolean doOnlyOnRelocating = false;
                if (shard.primary()) {
                    if (shard.relocating()) {  //主分片正在迁移
                        doOnlyOnRelocating = true;
                    } else {
                        continue;
                    }
                }
                // we index on a replica that is initializing as well since we might not have got the event
                // yet that it was started. We will get an exception IllegalShardState exception if its not started
                // and that's fine, we will ignore it
                if (!doOnlyOnRelocating) {//非主分片执行
                    performOnReplica(response, counter, shard, shard.currentNodeId(), indexMetaData);
                }
                if (shard.relocating()) {//分片正在迁移  迁移到的节点relocatingNodeId  也要写
                    performOnReplica(response, counter, shard, shard.relocatingNodeId(), indexMetaData);
                }
            }

            // now do the postPrimary operation, and check if the listener needs to be invoked
            postPrimaryOperation(request, response);
            // we also invoke here in case replicas finish before postPrimaryAction does
            if (counter.decrementAndGet() == 0) {
                listener.onResponse(response.response());
            }
        }

        void performOnReplica(final PrimaryResponse<Response, ReplicaRequest> response, final AtomicInteger counter, final ShardRouting shard, String nodeId, final IndexMetaData indexMetaData) {
            // if we don't have that node, it means that it might have failed and will be created again, in
            // this case, we don't have to do the operation, and just let it failover
            if (!clusterState.nodes().nodeExists(nodeId)) {//检查此分片所在节点是否存在
                if (counter.decrementAndGet() == 0) {
                    listener.onResponse(response.response());
                }
                return;
            }

            final ReplicaOperationRequest shardRequest = new ReplicaOperationRequest(shardIt.shardId().id(), response.replicaRequest());
            if (!nodeId.equals(clusterState.nodes().localNodeId())) {//此分片所在节点不是本地节点
                DiscoveryNode node = clusterState.nodes().get(nodeId);
                transportService.sendRequest(node, transportReplicaAction, shardRequest, transportOptions, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty vResponse) {
                        finishIfPossible();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (!ignoreReplicaException(exp.unwrapCause())) {
                            logger.warn("Failed to perform " + transportAction + " on replica " + shardIt.shardId(), exp);
                            shardStateAction.shardFailed(shard, indexMetaData.getUUID(),
                                    "Failed to perform [" + transportAction + "] on replica, message [" + detailedMessage(exp) + "]");
                        }
                        finishIfPossible();
                    }

                    private void finishIfPossible() {
                        if (counter.decrementAndGet() == 0) {
                            listener.onResponse(response.response());
                        }
                    }
                });
            } else {//是本地节点
                if (request.operationThreaded()) {   //线程执行
                    request.beforeLocalFork();
                    try {
                        threadPool.executor(executor).execute(new AbstractRunnable() {
                            @Override
                            public void run() {
                                try {
                                    shardOperationOnReplica(shardRequest);//副本操作
                                } catch (Throwable e) {
                                    if (!ignoreReplicaException(e)) {
                                        logger.warn("Failed to perform " + transportAction + " on replica " + shardIt.shardId(), e);
                                        shardStateAction.shardFailed(shard, indexMetaData.getUUID(),
                                                "Failed to perform [" + transportAction + "] on replica, message [" + detailedMessage(e) + "]");
                                    }
                                }
                                if (counter.decrementAndGet() == 0) {  //所有副本都写入成功
                                    listener.onResponse(response.response());
                                }
                            }

                            // we must never reject on because of thread pool capacity on replicas
                            @Override
                            public boolean isForceExecution() {
                                return true;
                            }
                        });
                    } catch (Throwable e) {
                        if (!ignoreReplicaException(e)) {
                            logger.warn("Failed to perform " + transportAction + " on replica " + shardIt.shardId(), e);
                            shardStateAction.shardFailed(shard, indexMetaData.getUUID(),
                                    "Failed to perform [" + transportAction + "] on replica, message [" + detailedMessage(e) + "]");
                        }
                        // we want to decrement the counter here, in teh failure handling, cause we got rejected
                        // from executing on the thread pool
                        if (counter.decrementAndGet() == 0) {
                            listener.onResponse(response.response());
                        }
                    }
                } else {
                    try {
                        shardOperationOnReplica(shardRequest);
                    } catch (Throwable e) {
                        if (!ignoreReplicaException(e)) {
                            logger.warn("Failed to perform " + transportAction + " on replica" + shardIt.shardId(), e);
                            shardStateAction.shardFailed(shard, indexMetaData.getUUID(),
                                    "Failed to perform [" + transportAction + "] on replica, message [" + detailedMessage(e) + "]");
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        listener.onResponse(response.response());
                    }
                }
            }
        }
    }

    public static class PrimaryResponse<Response, ReplicaRequest> {
        private final ReplicaRequest replicaRequest;
        private final Response response;
        private final Object payload;

        public PrimaryResponse(ReplicaRequest replicaRequest, Response response, Object payload) {
            this.replicaRequest = replicaRequest;
            this.response = response;
            this.payload = payload;
        }

        public ReplicaRequest replicaRequest() {
            return this.replicaRequest;
        }

        public Response response() {
            return response;
        }

        public Object payload() {
            return payload;
        }
    }
}
