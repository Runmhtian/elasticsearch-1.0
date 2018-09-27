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

package org.elasticsearch.action.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 进行 index操作的入口， 索引文档
 *
 *
 * Performs the index operation.
 * <p/>
 * <p>Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to <tt>true</tt>, will automatically create an index if one does not exists.
 * Defaults to <tt>true</tt>.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to <tt>true</tt>.
 * </ul>
 */
public class TransportIndexAction extends TransportShardReplicationOperationAction<IndexRequest, IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final TransportCreateIndexAction createIndexAction;

    private final MappingUpdatedAction mappingUpdatedAction;

    private final boolean waitForMappingChange;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.waitForMappingChange = settings.getAsBoolean("action.wait_on_mapping_change", false);
    }

    @Override
    protected void doExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) { // 判断请求的index是否需要自动创建
            request.beforeLocalFork(); // we fork on another thread...
            createIndexAction.execute(new CreateIndexRequest(request.index()).cause("auto(index api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);  //自动创建后再去执行，忽略了创建索引的响应
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(request, listener);
                        } catch (Throwable e1) {
                            listener.onFailure(e1);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {// index
            innerExecute(request, listener);
        }
    }

    @Override
    protected boolean resolveRequest(ClusterState state, IndexRequest request, ActionListener<IndexResponse> indexResponseActionListener) {
        MetaData metaData = clusterService.state().metaData();
        String aliasOrIndex = request.index();
        request.index(metaData.concreteIndex(request.index()));
        MappingMetaData mappingMd = null;
        if (metaData.hasIndex(request.index())) {
            mappingMd = metaData.index(request.index()).mappingOrDefault(request.type());
        }
        request.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
        return true;
    }

    private void innerExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected IndexRequest newRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexRequest newReplicaRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected String transportAction() {
        return IndexAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    /*
    根据ruquest信息 获取分片迭代器（同一个分片的所有实例  主分片 备份分片）  PlainOperationRouting=clusterService.operationRouting()
     */
    @Override
    protected ShardIterator shards(ClusterState clusterState, IndexRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
    }

    // index 的主分片操作
    @Override
    protected PrimaryResponse<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final IndexRequest request = shardRequest.request;

        // validate, if routing is required, that we got routing
        // 根据index 和type 获取mappingmetadata
        IndexMetaData indexMetaData = clusterState.metaData().index(request.index());
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        /*
        "_routing": {
            "required": true,
            "path":"_routing"
        },
        mapping中 有routing配置，插入的时候必须执行_routing

         */
        if (mappingMd != null && mappingMd.routing().required()) {
            if (request.routing() == null) {
                throw new RoutingMissingException(request.index(), request.type(), request.id());
            }
        }
        //得到IndexShard对象
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        //封装请求
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        long version;
        boolean created;
        Engine.IndexingOperation op;
        if (request.opType() == IndexRequest.OpType.INDEX) {   //索引 index ，若有相同文档Id，则替代
            Engine.Index index = indexShard.prepareIndex(sourceToParse)
                    .version(request.version())
                    .versionType(request.versionType())
                    .origin(Engine.Operation.Origin.PRIMARY);
            if (index.parsedDoc().mappingsModified()) {  // 索引的文档 造成mapping修改
                updateMappingOnMaster(request, indexMetaData);
            }
            indexShard.index(index);
            version = index.version();
            op = index;
            created = index.created();
        } else {  //create  直接添加到索引中，若有相同的文档ID，则不去替代，返回信息
            //https://www.elastic.co/guide/en/elasticsearch/guide/current/create-doc.html
            Engine.Create create = indexShard.prepareCreate(sourceToParse)  //得到一个create对象 封装了文档相关信息
                    .version(request.version()) //请求的文档版本
                    .versionType(request.versionType())  //请求的文档版本类型
                    .origin(Engine.Operation.Origin.PRIMARY); //主分片操作
            if (create.parsedDoc().mappingsModified()) {
                updateMappingOnMaster(request, indexMetaData);
            }
            indexShard.create(create);  // 执行索引
            version = create.version();
            op = create;
            created = true;
        }
        if (request.refresh()) {  //主动refresh
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_index").force(false));
            } catch (Throwable e) {
                // ignore
            }
        }

        // update the version on the request, so it will be used for the replicas
        request.version(version);

        IndexResponse response = new IndexResponse(request.index(), request.type(), request.id(), version, created);
        return new PrimaryResponse<IndexResponse, IndexRequest>(shardRequest.request, response, op);
    }

    //在单个副本分片上的操作   与在主分片上的操作基本一致
    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        IndexRequest request = shardRequest.request;
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        if (request.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse)
                    .version(request.version())
                    .origin(Engine.Operation.Origin.REPLICA);
            indexShard.index(index);
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse)
                    .version(request.version())
                    .origin(Engine.Operation.Origin.REPLICA);
            indexShard.create(create);
        }
        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_index").force(false));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void updateMappingOnMaster(final IndexRequest request, IndexMetaData indexMetaData) {
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            final MapperService mapperService = indicesService.indexServiceSafe(request.index()).mapperService();
            final DocumentMapper documentMapper = mapperService.documentMapper(request.type());
            if (documentMapper == null) { // should not happen
                return;
            }
            // we generate the order id before we get the mapping to send and refresh the source, so
            // if 2 happen concurrently, we know that the later order will include the previous one
            long orderId = mappingUpdatedAction.generateNextMappingUpdateOrder();
            documentMapper.refreshSource();
            DiscoveryNode node = clusterService.localNode();
            final MappingUpdatedAction.MappingUpdatedRequest mappingRequest =
                    new MappingUpdatedAction.MappingUpdatedRequest(request.index(), indexMetaData.uuid(), request.type(), documentMapper.mappingSource(), orderId, node != null ? node.id() : null);
            logger.trace("Sending mapping updated to master: {}", mappingRequest);
            mappingUpdatedAction.execute(mappingRequest, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override
                public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    latch.countDown();
                    logger.warn("Failed to update master on updated mapping for {}", e, mappingRequest);
                }
            });
        } catch (Exception e) {
            latch.countDown();
            logger.warn("Failed to update master on updated mapping for index [" + request.index() + "], type [" + request.type() + "]", e);
        }

        if (waitForMappingChange) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
