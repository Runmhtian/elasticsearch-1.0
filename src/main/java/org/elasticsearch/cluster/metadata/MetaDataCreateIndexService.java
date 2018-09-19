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

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Service responsible for submitting create index requests
 */
public class MetaDataCreateIndexService extends AbstractComponent {

    private final Environment environment;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final MetaDataService metaDataService;
    private final Version version;
    private final String riverIndexName;

    @Inject
    public MetaDataCreateIndexService(Settings settings, Environment environment, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                      AllocationService allocationService, MetaDataService metaDataService, Version version, @RiverIndexName String riverIndexName) {
        super(settings);
        this.environment = environment;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.metaDataService = metaDataService;
        this.version = version;
        this.riverIndexName = riverIndexName;
    }
    // TransportCreateIndexAction 调用此service来创建索引
    public void createIndex(final CreateIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : request.settings().getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) { // 自动添加前缀
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        request.settings(updatedSettingsBuilder.build());//构建索引setting

        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling
        //根据此索引名称，获得信号量   许可数量为1  也就是相当于一个互斥锁，防止同时创建相同的索引名称的索引
        final Semaphore mdLock = metaDataService.indexMetaDataLock(request.index());

        // quick check to see if we can acquire a lock, otherwise spawn to a thread pool
        //获得许可，若是获得则创建索引，否则扔给线程池management
        if (mdLock.tryAcquire()) {
            createIndex(request, listener, mdLock);
            return;
        }

        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取锁等待时间
                    if (!mdLock.tryAcquire(request.masterNodeTimeout().nanos(), TimeUnit.NANOSECONDS)) {
                        //超出等待时间响应失败
                        listener.onFailure(new ProcessClusterEventTimeoutException(request.masterNodeTimeout(), "acquire index lock"));
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    listener.onFailure(e);
                    return;
                }
                //获取到了锁，创建索引
                createIndex(request, listener, mdLock);
            }
        });
    }

    public void validateIndexName(String index, ClusterState state) throws ElasticsearchException {
        if (state.routingTable().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (state.metaData().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (!Strings.validFileName(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain '#'");
        }
        if (!index.equals(riverIndexName) && index.charAt(0) == '_') {
            throw new InvalidIndexNameException(new Index(index), index, "must not start with '_'");
        }
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must be lowercase");
        }
        if (state.metaData().aliases().containsKey(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "already exists as alias");
        }
    }

    private void createIndex(final CreateIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener listener, final Semaphore mdLock) {
        //优先级URGENT
        clusterService.submitStateUpdateTask("create-index [" + request.index() + "], cause [" + request.cause() + "]", Priority.URGENT, new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }  //必须ack

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                mdLock.release();
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                mdLock.release();
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                mdLock.release();
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                boolean indexCreated = false;
                String failureReason = null;
                try {
                    validate(request, currentState);//验证索引名称

                    // we only find a template when its an API call (a new index)
                    // find templates, highest order are better matching
                    // 查找模板
                    List<IndexTemplateMetaData> templates = findTemplates(request, currentState);

                    Map<String, Custom> customs = Maps.newHashMap();

                    // add the request mapping
                    Map<String, Map<String, Object>> mappings = Maps.newHashMap();

                    for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                        mappings.put(entry.getKey(), parseMapping(entry.getValue()));  //解析mapping
                    }

                    for (Map.Entry<String, Custom> entry : request.customs().entrySet()) {
                        customs.put(entry.getKey(), entry.getValue());
                    }
                    /*
                    mapping 包含以下几部分
                    1.request的mapping
                    2.匹配到的templates
                    3.home/config/mappings/indexName 下的mapping
                    4.home/config/mappings/_default下的mapping
                    5._default_ 的mapping type (也就是索引类型 也叫mappings类型)
                    优先级从高到底

                    虽然3,4在这里是最后addMappings，但是只会添加和合并，没有的key添加，list类型合并。

                     */
                    // apply templates, merging the mappings into the request mapping if exists
                    // 根据mapping  和template  合并 mapping   还有custom
                    for (IndexTemplateMetaData template : templates) {
                        for (ObjectObjectCursor<String, CompressedString> cursor : template.mappings()) {
                            if (mappings.containsKey(cursor.key)) {
                                XContentHelper.mergeDefaults(mappings.get(cursor.key), parseMapping(cursor.value.string()));
                            } else {
                                mappings.put(cursor.key, parseMapping(cursor.value.string()));
                            }
                        }
                        // handle custom
                        for (ObjectObjectCursor<String, Custom> cursor : template.customs()) {
                            String type = cursor.key;
                            IndexMetaData.Custom custom = cursor.value;
                            IndexMetaData.Custom existing = customs.get(type);
                            if (existing == null) {
                                customs.put(type, custom);
                            } else {
                                IndexMetaData.Custom merged = IndexMetaData.lookupFactorySafe(type).merge(existing, custom);
                                customs.put(type, merged);
                            }
                        }
                    }

                    // now add config level mappings
                    File mappingsDir = new File(environment.configFile(), "mappings");
                    if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                        // first index level
                        // 找对应索引的config文件
                        File indexMappingsDir = new File(mappingsDir, request.index());
                        if (indexMappingsDir.exists() && indexMappingsDir.isDirectory()) {
                            addMappings(mappings, indexMappingsDir);
                        }

                        // second is the _default mapping   默认mapping配置文件添加
                        File defaultMappingsDir = new File(mappingsDir, "_default");
                        if (defaultMappingsDir.exists() && defaultMappingsDir.isDirectory()) {
                            addMappings(mappings, defaultMappingsDir);
                        }
                    }
                    //合并索引的setting
                    ImmutableSettings.Builder indexSettingsBuilder = settingsBuilder();
                    // apply templates, here, in reverse order, since first ones are better matching
                    for (int i = templates.size() - 1; i >= 0; i--) {
                        indexSettingsBuilder.put(templates.get(i).settings());
                    }
                    // now, put the request settings, so they override templates
                    indexSettingsBuilder.put(request.settings());
                    /*
                    index setting
                    主要是SETTING_NUMBER_OF_SHARDS与SETTING_NUMBER_OF_REPLICAS

                    1.request中的indexSetting
                    2.templates中的setting
                    3.ES setting中的配置的默认值
                    4.默认值
                    优先级从高到低

                     */
                    if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                        if (request.index().equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                        }
                    }
                    if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                        if (request.index().equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                        }
                    }

                    if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                        indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
                    }
                    // version
                    if (indexSettingsBuilder.get(SETTING_VERSION_CREATED) == null) {
                        indexSettingsBuilder.put(SETTING_VERSION_CREATED, version);
                    }
                    // uuid
                    indexSettingsBuilder.put(SETTING_UUID, Strings.randomBase64UUID());

                    Settings actualIndexSettings = indexSettingsBuilder.build();
                    //准备工作完成
                    // Set up everything, now locally create the index to see that things are ok, and apply

                    // create the index here (on the master) to validate it can be created, as well as adding the mapping
                    // 创建索引  只需要使用索引名称  和索引配置
                    indicesService.createIndex(request.index(), actualIndexSettings, clusterService.localNode().id());
                    indexCreated = true;
                    // now add the mappings 得到索引对应的 IndexService  包含了索引相关的各个服务模块
                    IndexService indexService = indicesService.indexServiceSafe(request.index());
                    //拿到mapping服务
                    MapperService mapperService = indexService.mapperService();
                    // first, add the default mapping
                    if (mappings.containsKey(MapperService.DEFAULT_MAPPING)) {  //_default_
                        /*
                        https://www.elastic.co/guide/en/elasticsearch/reference/2.1/default-mapping.html#default-mapping
                        默认mapping（将用作任何新mapping类型的基本映射）可以通过向索引添加名称为_default_的mapping类型来自定义，
                        无论是在创建索引时还是稍后使用PUT mapping API。
                         */
                        try {
                            // 将default 添加到mapperService中
                            mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedString(XContentFactory.jsonBuilder().map(mappings.get(MapperService.DEFAULT_MAPPING)).string()), false);
                        } catch (Exception e) {
                            failureReason = "failed on parsing default mapping on index creation";
                            throw new MapperParsingException("mapping [" + MapperService.DEFAULT_MAPPING + "]", e);
                        }
                    }
                    for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
                        if (entry.getKey().equals(MapperService.DEFAULT_MAPPING)) {
                            continue;
                        }
                        try {
                            // apply the default here, its the first time we parse it
                            //添加其他 mapping配置，到mapperService中
                            mapperService.merge(entry.getKey(), new CompressedString(XContentFactory.jsonBuilder().map(entry.getValue()).string()), true);
                        } catch (Exception e) {
                            failureReason = "failed on parsing mappings on index creation";
                            throw new MapperParsingException("mapping [" + entry.getKey() + "]", e);
                        }
                    }
                    // now, update the mappings with the actual source
                    // key应该是type  mapping元数据
                    Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
                    for (DocumentMapper mapper : mapperService) {
                        MappingMetaData mappingMd = new MappingMetaData(mapper);
                        mappingsMetaData.put(mapper.type(), mappingMd);
                    }
                    // 索引元数据builder  setting mapping等
                    final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(request.index()).settings(actualIndexSettings);
                    for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                        indexMetaDataBuilder.putMapping(mappingMd);
                    }
                    for (Map.Entry<String, Custom> customEntry : customs.entrySet()) {
                        indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
                    }
                    indexMetaDataBuilder.state(request.state());
                    final IndexMetaData indexMetaData;
                    try {
                        indexMetaData = indexMetaDataBuilder.build();
                    } catch (Exception e) {
                        failureReason = "failed to build index metadata";
                        throw e;
                    }
                    // 将索引元数据  添加到 metadata  indexMetaData-->metadata
                    MetaData newMetaData = MetaData.builder(currentState.metaData())
                            .put(indexMetaData, false)
                            .build();

                    logger.info("[{}] creating index, cause [{}], shards [{}]/[{}], mappings {}", request.index(), request.cause(), indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), mappings.keySet());

                    // 集群阻塞 等级
                    /*
                    write read metadata 三种类型阻塞
                                            block
                    index read-only  / write  metadata
                    index read  / read
                    index write /write
                    index metadata /metadata
                    index close /read write
                     */
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    if (!request.blocks().isEmpty()) {//
                        for (ClusterBlock block : request.blocks()) {
                            blocks.addIndexBlock(request.index(), block); //添加索创建索引的请求中有限制引对应的限制
                        }
                    }
                    if (request.state() == State.CLOSE) {  //close  关闭此索引的读写
                        blocks.addIndexBlock(request.index(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
                    }
                    // 更新metaData  和 block 到ClusterState 中
                    ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

                    if (request.state() == State.OPEN) {
                        //routingTable
                        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                                .addAsNew(updatedState.metaData().index(request.index()));
                        // 更新routingTable 到ClusterState中  然后 分片分配
                        RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(routingTableBuilder).build());
                        //加入分配结果
                        updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();

                        //分配之后shardRouting 中分片的状态为 INITIALIZING   准备开始初始化
                    }
                    return updatedState;
                } finally {
                    if (indexCreated) {
                        // Index was already partially created - need to clean up
                        indicesService.removeIndex(request.index(), failureReason != null ? failureReason : "failed to create index");
                    }
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        return XContentFactory.xContent(mappingSource).createParser(mappingSource).mapAndClose();
    }

    private void addMappings(Map<String, Map<String, Object>> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            if (mappingFile.isHidden()) {
                continue;
            }
            int lastDotIndex = mappingFile.getName().lastIndexOf('.');
            String mappingType = lastDotIndex != -1 ? mappingFile.getName().substring(0, lastDotIndex) : mappingFile.getName();
            try {
                String mappingSource = Streams.copyToString(new InputStreamReader(new FileInputStream(mappingFile), Charsets.UTF_8));
                if (mappings.containsKey(mappingType)) {
                    XContentHelper.mergeDefaults(mappings.get(mappingType), parseMapping(mappingSource));
                } else {
                    mappings.put(mappingType, parseMapping(mappingSource));
                }
            } catch (Exception e) {
                logger.warn("failed to read / parse mapping [" + mappingType + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    private List<IndexTemplateMetaData> findTemplates(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        List<IndexTemplateMetaData> templates = Lists.newArrayList();
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;
            if (Regex.simpleMatch(template.template(), request.index())) {// 索引名称正则匹配
                templates.add(template);
            }
        }

        // see if we have templates defined under config
        File templatesDir = new File(environment.configFile(), "templates"); //配置文件下的模板
        if (templatesDir.exists() && templatesDir.isDirectory()) {
            File[] templatesFiles = templatesDir.listFiles();
            if (templatesFiles != null) {
                for (File templatesFile : templatesFiles) {
                    XContentParser parser = null;
                    try {
                        byte[] templatesData = Streams.copyToByteArray(templatesFile);
                        parser = XContentHelper.createParser(templatesData, 0, templatesData.length);
                        IndexTemplateMetaData template = IndexTemplateMetaData.Builder.fromXContent(parser);
                        if (Regex.simpleMatch(template.template(), request.index())) {
                            templates.add(template);
                        }
                    } catch (Exception e) {
                        logger.warn("[{}] failed to read template [{}] from config", e, request.index(), templatesFile.getAbsolutePath());
                    } finally {
                        IOUtils.closeWhileHandlingException(parser);
                    }
                }
            }
        }
        //按照顺序排序
        CollectionUtil.timSort(templates, new Comparator<IndexTemplateMetaData>() {
            @Override
            public int compare(IndexTemplateMetaData o1, IndexTemplateMetaData o2) {
                return o2.order() - o1.order();
            }
        });
        return templates;
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) throws ElasticsearchException {
        validateIndexName(request.index(), state);
    }
}
