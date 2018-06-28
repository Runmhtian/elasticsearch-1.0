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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.action.index.*;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationModule;
import org.elasticsearch.cluster.routing.operation.OperationRoutingModule;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;

/**
 *
 */
public class ClusterModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public ClusterModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new AllocationModule(settings),
                new OperationRoutingModule(settings),
                new ClusterDynamicSettingsModule(),
                new IndexDynamicSettingsModule());
    }

    @Override
    protected void configure() {
        /*
        node.data  node.client      如果是client，则设置 node.data=false
         */
        bind(DiscoveryNodeService.class).asEagerSingleton();
        /*
        InternalClusterService(Settings settings, DiscoveryService discoveryService, OperationRouting operationRouting, TransportService transportService,
                                  NodeSettingsService nodeSettingsService, ThreadPool threadPool)
         */
        bind(ClusterService.class).to(InternalClusterService.class).asEagerSingleton();
        // 初始化信号量Semaphore
        bind(MetaDataService.class).asEagerSingleton();
        // 创建索引
        bind(MetaDataCreateIndexService.class).asEagerSingleton();
        // 删除索引
        bind(MetaDataDeleteIndexService.class).asEagerSingleton();
        //Service responsible for submitting open/close index requests
        bind(MetaDataIndexStateService.class).asEagerSingleton();
        // mapping 请求
        bind(MetaDataMappingService.class).asEagerSingleton();
        // 索引别名
        bind(MetaDataIndexAliasesService.class).asEagerSingleton();
        // update index
        bind(MetaDataUpdateSettingsService.class).asEagerSingleton();
        // 索引模板
        bind(MetaDataIndexTemplateService.class).asEagerSingleton();

        // 根据 集群状态更新事件  来判断是否需要  reRoute
        bind(RoutingService.class).asEagerSingleton();
        //向主节点发送  分片状态信息  shardRoutingEntry    ShardRouting(分片对象)
        bind(ShardStateAction.class).asEagerSingleton();
        // 向主节点发送  索引删除请求
        bind(NodeIndexDeletedAction.class).asEagerSingleton();
        // 向主节点发送 mapping 刷新
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        //Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
// * in the cluster state meta data (and broadcast to all members).
        bind(MappingUpdatedAction.class).asEagerSingleton();
       //运行在所有节点上，但是只有主节点有效
        // 监听数据节点数量  一旦有变化  产生一个ClusterInfoUpdateJob
        // 主节点在运行，当其他节点成为主节点时  通过onMaster方法是  isMaster属性为true
        bind(ClusterInfoService.class).to(InternalClusterInfoService.class).asEagerSingleton();
    }
}