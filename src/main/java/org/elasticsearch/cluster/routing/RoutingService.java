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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Future;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A {@link RoutingService} listens to clusters state. When this service
 * receives a {@link ClusterChangedEvent} the cluster state will be verified and
 * the routing tables might be updated.
 * <p>
 * Note: The {@link RoutingService} is responsible for cluster wide operations
 * that include modifications to the cluster state. Such an operation can only
 * be performed on the clusters master node. Unless the local node this service
 * is running on is the clusters master node this service will not perform any
 * actions.
 *
 * 根据集群状态 更新  routing tables
 *
 * </p>
 */
public class RoutingService extends AbstractLifecycleComponent<RoutingService> implements ClusterStateListener {

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "routing-table-updater";

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final TimeValue schedule;

    private volatile boolean routingTableDirty = false;

    private volatile Future scheduledRoutingTableFuture;

    @Inject
    public RoutingService(Settings settings, ThreadPool threadPool, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.schedule = componentSettings.getAsTime("schedule", timeValueSeconds(10));
        clusterService.addFirst(this);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if (scheduledRoutingTableFuture != null) {
            scheduledRoutingTableFuture.cancel(true);
            scheduledRoutingTableFuture = null;
        }
        clusterService.remove(this);
    }
    /*
    根据ClusterChangedEvent   来看是否需要
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.source().equals(CLUSTER_UPDATE_TASK_SOURCE)) {
            // that's us, ignore this event
            return;
        }
        if (event.state().nodes().localNodeMaster()) {  //  是master 节点
            // we are master, schedule the routing table updater
            if (scheduledRoutingTableFuture == null) {
                // a new master (us), make sure we reroute shards
                routingTableDirty = true;
                // 按照固定间隔执行  RoutingTableUpdater
                scheduledRoutingTableFuture = threadPool.scheduleWithFixedDelay(new RoutingTableUpdater(), schedule);
            }
            if (event.nodesRemoved()) { // 节点移除event
                // if nodes were removed, we don't want to wait for the scheduled task
                // since we want to get primary election as fast as possible
                routingTableDirty = true;
                reroute();  // 重新分配路由
                // Commented out since we make sure to reroute whenever shards changes state or metadata changes state
//            } else if (event.routingTableChanged()) {
//                routingTableDirty = true;
//                reroute();
            } else {
                if (event.nodesAdded()) { // 节点添加event
                    for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
                        if (node.dataNode()) {  // 添加的节点中有数据节点  routingTableDirty = true
                            routingTableDirty = true;
                            break;
                        }
                    }
                }
            }
        } else {
            if (scheduledRoutingTableFuture != null) {
                scheduledRoutingTableFuture.cancel(true);
                scheduledRoutingTableFuture = null;
            }
        }
    }

    private void reroute() {
        try {
            if (!routingTableDirty) {  // 不是脏 路由表
                return;
            }
            if (lifecycle.stopped()) {  // 组件生命周期 结束
                return;
            }
            //Submits a task that will update the cluster state.
            clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE, Priority.HIGH, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    RoutingAllocation.Result routingResult = allocationService.reroute(currentState);
                    if (!routingResult.changed()) {
                        // no state changed
                        return currentState;
                    }
                    return ClusterState.builder(currentState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
            routingTableDirty = false;
        } catch (Exception e) {
            logger.warn("Failed to reroute routing table", e);
        }
    }

    private class RoutingTableUpdater implements Runnable {

        @Override
        public void run() {
            reroute();
        }
    }
}
