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

package org.elasticsearch.discovery.zen;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.MasterFaultDetection;
import org.elasticsearch.discovery.zen.fd.NodesFaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class ZenDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private AllocationService allocationService;
    private final ClusterName clusterName;
    private final DiscoveryNodeService discoveryNodeService;
    /**
     * 节点心跳
     */
    private final ZenPingService pingService;
    private final MasterFaultDetection masterFD;
    private final NodesFaultDetection nodesFD;
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership;
    private final Version version;


    private final TimeValue pingTimeout;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    /**
     * 节点选举
     */
    private final ElectMasterService electMaster;

    private final boolean masterElectionFilterClientNodes;
    private final boolean masterElectionFilterDataNodes;


    private DiscoveryNode localNode;

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();


    @Nullable
    private NodeService nodeService;

    @Inject
    public ZenDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                        TransportService transportService, ClusterService clusterService, NodeSettingsService nodeSettingsService,
                        DiscoveryNodeService discoveryNodeService, ZenPingService pingService, Version version) {
        super(settings);
        this.clusterName = clusterName;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.discoveryNodeService = discoveryNodeService;
        this.pingService = pingService;
        this.version = version;

        // also support direct discovery.zen settings, for cases when it gets extended
        this.pingTimeout = settings.getAsTime("discovery.zen.ping.timeout", settings.getAsTime("discovery.zen.ping_timeout", componentSettings.getAsTime("ping_timeout", componentSettings.getAsTime("initial_ping_timeout", timeValueSeconds(3)))));
        this.sendLeaveRequest = componentSettings.getAsBoolean("send_leave_request", true);

        this.masterElectionFilterClientNodes = settings.getAsBoolean("discovery.zen.master_election.filter_client", true);
        this.masterElectionFilterDataNodes = settings.getAsBoolean("discovery.zen.master_election.filter_data", false);

        logger.debug("using ping.timeout [{}], master_election.filter_client [{}], master_election.filter_data [{}]", pingTimeout, masterElectionFilterClientNodes, masterElectionFilterDataNodes);

        this.electMaster = new ElectMasterService(settings);
        nodeSettingsService.addListener(new ApplySettings());

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService);
        this.nodesFD.addListener(new NodeFailureListener());

        //初始化 发布集群状态
        this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
        this.pingService.setNodesProvider(this);
        this.membership = new MembershipAction(settings, transportService, this, new MembershipListener());
        //注册重新加入集群的 handler和action
        transportService.registerHandler(RejoinClusterRequestHandler.ACTION, new RejoinClusterRequestHandler());
    }

    @Override
    public void setNodeService(@Nullable NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        //根据配置文件 得到当前节点的  节点配置
        Map<String, String> nodeAttributes = discoveryNodeService.buildAttributes();
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        // 随机生成一个nodeId
        final String nodeId = DiscoveryService.generateNodeId(settings);
        // new DiscoveryNode
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes, version);
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        // 加入到故障检测
        nodesFD.updateNodes(latestDiscoNodes);
        //开始 ping服务
        pingService.start();

        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        // 异步加入集群，DiscoveryService无论如何都会等待30秒才会被发现  新节点启动 会选举新的master

        /*
        这里30s后加入集群是很有必要的，让节点先可以被ping到，但是不能够加入集群， 其他节点正好选完master，但是还没有形成集群  30s足够 让master节点完成初始化并把集群状态同步到其他节点

        若是没有这30s， 节点ABC，A 被选为master，BC阻塞，A正在初始化的时候，D刚好启动，D findMaster 发现自己Id最小，也进行master的初始化
        这样可能会出现问题
        而通过节点启动时 延迟30s 避免了这样情况的发生

          实际上就是避免  原集群刚运行完findMaster ，新加入的节点直接去运行findMaster 造成不一致
          通过延迟30s，新加入的节点会在30s 后再去运行findMaster 就不会出现问题

        而若是新加入的节点在 原集群运行findMaster之前启动，则其他节点都会无线循环  等待新加入节点在30s后运行发现自己是master，然后进行初始化

        这里都假设新加入的节点id 是最小的。

         */
        asyncJoinCluster();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        pingService.stop();
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        initialStateSent.set(false);
        if (sendLeaveRequest) {
            if (!master && latestDiscoNodes.masterNode() != null) {
                try {
                    membership.sendLeaveRequestBlocking(latestDiscoNodes.masterNode(), localNode, TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug("failed to send leave request to master [{}]", e, latestDiscoNodes.masterNode());
                }
            } else {
                DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(latestDiscoNodes.nodes().values(), 5);
                for (DiscoveryNode possibleMaster : possibleMasters) {
                    if (localNode.equals(possibleMaster)) {
                        continue;
                    }
                    try {
                        membership.sendLeaveRequest(latestDiscoNodes.masterNode(), possibleMaster);
                    } catch (Exception e) {
                        logger.debug("failed to send leave request from master [{}] to possible master [{}]", e, latestDiscoNodes.masterNode(), possibleMaster);
                    }
                }
            }
        }
        master = false;
        if (currentJoinThread != null) {
            try {
                currentJoinThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        masterFD.close();
        nodesFD.close();
        publishClusterState.close();
        membership.close();
        pingService.close();
    }

    @Override
    public DiscoveryNode localNode() {
        return localNode;
    }

    @Override
    public void addListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.add(listener);
    }

    @Override
    public void removeListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.remove(listener);
    }

    @Override
    public String nodeDescription() {
        return clusterName.value() + "/" + localNode.id();
    }

    @Override
    public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return DiscoveryNodes.builder().put(localNode).localNodeId(localNode.id()).build();
    }

    @Override
    public NodeService nodeService() {
        return this.nodeService;
    }

    /**
     * 集群状态改变时  调用  更新latestDiscoNodes  和nodesFD   只有master会调用
     * @param clusterState
     * @param ackListener
     */
    @Override
    public void publish(ClusterState clusterState, AckListener ackListener) {
        if (!master) {
            throw new ElasticsearchIllegalStateException("Shouldn't publish state when not master");
        }
        latestDiscoNodes = clusterState.nodes();
        nodesFD.updateNodes(clusterState.nodes());
        publishClusterState.publish(clusterState, ackListener);
    }



    private void asyncJoinCluster() {
        if (currentJoinThread != null) {
            // we are already joining, ignore...
            logger.trace("a join thread already running");
            return;
        }
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innerJoinCluster();
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    /**
     * 加入集群
     *
     * 若是先启动三个节点，然后三个节点中  当运行到 findMaster时 节点A 被选为master， 节点BC 阻塞 ，这是新的节点D 加入，且节点ID最小，节点D
     *
     * 因为节点在启动30s后才会 加入集群，因此节点Bc再次findMaster时 master还不是自己， 而节点D 还没有加入集群，只是能够ping到，A节点提交了状态更新请求
     * B c 接收包含master信息的ping响应后，findMaster结果为A，然后形成一个A为master，b，c为普通node的集群，30s过后D加入集群，ping 响应中
     * A是master，因此findMaster的结果还是A
     *
     *
     */
    private void innerJoinCluster() {
        boolean retry = true;
        while (retry) {  // 循环  当没有master节点时，等待成为master的节点完成初始化，再去findMaster  就能够得到PingMaster，然后选出master
            if (lifecycle.stoppedOrClosed()) {
                return;
            }
            retry = false;
            // 得到 master节点，可能是选举得到的也可能是ping
            DiscoveryNode masterNode = findMaster();
            if (masterNode == null) {
                // 若是masterNode为null，则等待 nodeId最小的节点初始化完成，再去加入集群
                logger.trace("no masterNode returned");
                retry = true;
                continue;
            }
            //若是本地节点被选为 master  说明当前集群 其他节点在等待master初始化
            if (localNode.equals(masterNode)) {
                this.master = true;
                //开启普通节点故障检测
                nodesFD.start(); // start the nodes FD
                clusterService.submitStateUpdateTask("zen-disco-join (elected_as_master)", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                                .localNodeId(localNode.id())
                                .masterNodeId(localNode.id())  //集群状态中  自己的master节点。
                                        // put our local node
                                .put(localNode);
                        // update the fact that we are the master...
                        //初始化 DiscoveryNodes
                        latestDiscoNodes = builder.build();
                        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK).build();
                        return ClusterState.builder(currentState).nodes(latestDiscoNodes).blocks(clusterBlocks).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();   //调用listener
                    }
                });
            } else {  //local 不是master
                this.master = false;
                try {
                    // first, make sure we can connect to the master
                    transportService.connectToNode(masterNode);
                } catch (Exception e) {
                    logger.warn("failed to connect to master [{}], retrying...", e, masterNode);
                    retry = true;
                    continue;
                }
                // send join request
                try {
                    //发送请求加入
                    membership.sendJoinRequestBlocking(masterNode, localNode, pingTimeout);
                } catch (Exception e) {
                    if (e instanceof ElasticsearchException) {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, ((ElasticsearchException) e).getDetailedMessage());
                    } else {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, e.getMessage());
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace("detailed failed reason", e);
                    }
                    // failed to send the join request, retry
                    retry = true;
                    continue;
                }
                // master故障检测
                masterFD.start(masterNode, "initial_join");
                // no need to submit the received cluster state, we will get it from the master when it publishes
                // the fact that we joined
            }
        }
    }

    private void handleLeaveRequest(final DiscoveryNode node) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (master) {
            clusterService.submitStateUpdateTask("zen-disco-node_left(" + node + ")", Priority.URGENT, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes()).remove(node.id());
                    latestDiscoNodes = builder.build();
                    currentState = ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                    // check if we have enough master nodes, if not, we need to move into joining the cluster again
                    if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                        return rejoin(currentState, "not enough master nodes");
                    }
                    // eagerly run reroute to remove dead nodes from routing table
                    RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(currentState).build());
                    return ClusterState.builder(currentState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
        } else {
            handleMasterGone(node, "shut_down");
        }
    }

    /**
     * 非master节点故障时调用
     * @param node
     * @param reason
     */
    private void handleNodeFailure(final DiscoveryNode node, String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (!master) {
            // nothing to do here...
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-node_failed(" + node + "), reason " + reason, Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes())
                        .remove(node.id());  //去除这个node
                latestDiscoNodes = builder.build();
                currentState = ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                // master数目不够，  重新加入集群  asyncJoinCluster
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes");
                }
                // eagerly run reroute to remove dead nodes from routing table  更新routing table
                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(currentState).build());
                return ClusterState.builder(currentState).routingResult(routingResult).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                sendInitialStateEventIfNeeded();
            }
        });
    }

    private void handleMinimumMasterNodesChanged(final int minimumMasterNodes) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        final int prevMinimumMasterNode = ZenDiscovery.this.electMaster.minimumMasterNodes();
        ZenDiscovery.this.electMaster.minimumMasterNodes(minimumMasterNodes);
        if (!master) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-minimum_master_nodes_changed", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" + minimumMasterNodes + "]");
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                sendInitialStateEventIfNeeded();
            }
        });
    }

    /**
     * master Node 故障时调用
     *
     * 这里选举出来的非master节点  也调用了 submitStateUpdateTask
     * master节点在submitStateUpdateTask 中又会通过publish 到非master节点，再调用一次
     *
     * 不会出现重复调用么？
     *
     * @param masterNode
     * @param reason
     */
    private void handleMasterGone(final DiscoveryNode masterNode, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a master failure
            return;
        }
        if (master) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info("master_left [{}], reason [{}]", masterNode, reason);

        clusterService.submitStateUpdateTask("zen-disco-master_failed (" + masterNode + ")", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!masterNode.id().equals(currentState.nodes().masterNodeId())) {
                    // master got switched on us, no need to send anything
                    return currentState;
                }

                DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(currentState.nodes())
                        // make sure the old master node, which has failed, is not part of the nodes we publish
                        .remove(masterNode.id())
                        .masterNodeId(null).build();

                if (!electMaster.hasEnoughMasterNodes(discoveryNodes)) {
                    return rejoin(ClusterState.builder(currentState).nodes(discoveryNodes).build(), "not enough master nodes after master left (reason = " + reason + ")");
                }

                final DiscoveryNode electedMaster = electMaster.electMaster(discoveryNodes); // elect master
                if (localNode.equals(electedMaster)) {
                    master = true;
                    masterFD.stop("got elected as new master since master left (reason = " + reason + ")");
                    nodesFD.start();
                    discoveryNodes = DiscoveryNodes.builder(discoveryNodes).masterNodeId(localNode.id()).build();
                    latestDiscoNodes = discoveryNodes;
                    return ClusterState.builder(currentState).nodes(latestDiscoNodes).build();
                } else {
                    nodesFD.stop();
                    if (electedMaster != null) {
                        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).masterNodeId(electedMaster.id()).build();
                        masterFD.restart(electedMaster, "possible elected master since master left (reason = " + reason + ")");
                        latestDiscoNodes = discoveryNodes;
                        return ClusterState.builder(currentState)
                                .nodes(latestDiscoNodes)
                                .build();
                    } else {
                        return rejoin(ClusterState.builder(currentState).nodes(discoveryNodes).build(), "master_left and no other node elected to become master");
                    }
                }
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                sendInitialStateEventIfNeeded();
            }

        });
    }

    static class ProcessClusterState {
        final ClusterState clusterState;
        final PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed;
        volatile boolean processed;

        ProcessClusterState(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
            this.clusterState = clusterState;
            this.newStateProcessed = newStateProcessed;
        }
    }

    private final BlockingQueue<ProcessClusterState> processNewClusterStates = ConcurrentCollections.newBlockingQueue();


    //处理master 发布的集群状态更新请求
    void handleNewClusterStateFromMaster(ClusterState newClusterState, final PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
        if (master) {  // 当前节点是否是master
            final ClusterState newState = newClusterState;
            clusterService.submitStateUpdateTask("zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (newState.version() > currentState.version()) {
                        logger.warn("received cluster state from [{}] which is also master but with a newer cluster_state, rejoining to cluster...", newState.nodes().masterNode());
                        return rejoin(currentState, "zen-disco-master_receive_cluster_state_from_another_master [" + newState.nodes().masterNode() + "]");
                    } else {
                        logger.warn("received cluster state from [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster", newState.nodes().masterNode(), newState.nodes().masterNode());
                        transportService.sendRequest(newState.nodes().masterNode(), RejoinClusterRequestHandler.ACTION, new RejoinClusterRequest(currentState.nodes().localNodeId()), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override
                            public void handleException(TransportException exp) {
                                logger.warn("failed to send rejoin request to [{}]", exp, newState.nodes().masterNode());
                            }
                        });
                        return currentState;
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    newStateProcessed.onNewClusterStateProcessed();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                    newStateProcessed.onNewClusterStateFailed(t);
                }

            });
        } else { // 非master节点  接收到了其他节点的集群状态更新请求的处理
            if (newClusterState.nodes().localNode() == null) {
                logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", newClusterState.nodes().masterNode());
                newStateProcessed.onNewClusterStateFailed(new ElasticsearchIllegalStateException("received state from a node that is not part of the cluster"));
            } else {
                if (currentJoinThread != null) {
                    logger.debug("got a new state from master node, though we are already trying to rejoin the cluster");
                }

                final ProcessClusterState processClusterState = new ProcessClusterState(newClusterState, newStateProcessed);
                processNewClusterStates.add(processClusterState);

                // 同样会调用这个task
                clusterService.submitStateUpdateTask("zen-disco-receive(from master [" + newClusterState.nodes().masterNode() + "])", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // we already processed it in a previous event
                        if (processClusterState.processed) {
                            return currentState;
                        }

                        // TODO: once improvement that we can do is change the message structure to include version and masterNodeId
                        // at the start, this will allow us to keep the "compressed bytes" around, and only parse the first page
                        // to figure out if we need to use it or not, and only once we picked the latest one, parse the whole state


                        // try and get the state with the highest version out of all the ones with the same master node id
                        ProcessClusterState stateToProcess = processNewClusterStates.poll();
                        if (stateToProcess == null) {
                            return currentState;
                        }
                        stateToProcess.processed = true;
                        while (true) {
                            ProcessClusterState potentialState = processNewClusterStates.peek();
                            // nothing else in the queue, bail
                            if (potentialState == null) {
                                break;
                            }
                            // if its not from the same master, then bail
                            if (!Objects.equal(stateToProcess.clusterState.nodes().masterNodeId(), potentialState.clusterState.nodes().masterNodeId())) {
                                break;
                            }

                            // we are going to use it for sure, poll (remove) it
                            potentialState = processNewClusterStates.poll();
                            potentialState.processed = true;

                            if (potentialState.clusterState.version() > stateToProcess.clusterState.version()) {
                                // we found a new one
                                stateToProcess = potentialState;
                            }
                        }

                        ClusterState updatedState = stateToProcess.clusterState;

                        // if the new state has a smaller version, and it has the same master node, then no need to process it
                        // 新的状态 版本先现有的小  直接返回当前状态
                        if (updatedState.version() < currentState.version() && Objects.equal(updatedState.nodes().masterNodeId(), currentState.nodes().masterNodeId())) {
                            return currentState;
                        }

                        // we don't need to do this, since we ping the master, and get notified when it has moved from being a master
                        // because it doesn't have enough master nodes...
                        //if (!electMaster.hasEnoughMasterNodes(newState.nodes())) {
                        //    return disconnectFromCluster(newState, "not enough master nodes on new cluster state received from [" + newState.nodes().masterNode() + "]");
                        //}

                        latestDiscoNodes = updatedState.nodes();

                        // check to see that we monitor the correct master of the cluster
                        if (masterFD.masterNode() == null || !masterFD.masterNode().equals(latestDiscoNodes.masterNode())) {
                            //masterNode改变 重启master故障检测
                            masterFD.restart(latestDiscoNodes.masterNode(), "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
                        }

                        ClusterState.Builder builder = ClusterState.builder(updatedState);
                        // if the routing table did not change, use the original one
                        if (updatedState.routingTable().version() == currentState.routingTable().version()) {
                            builder.routingTable(currentState.routingTable());
                        }
                        // same for metadata
                        if (updatedState.metaData().version() == currentState.metaData().version()) {
                            builder.metaData(currentState.metaData());
                        } else {
                            // if its not the same version, only copy over new indices or ones that changed the version
                            MetaData.Builder metaDataBuilder = MetaData.builder(updatedState.metaData()).removeAllIndices();
                            for (IndexMetaData indexMetaData : updatedState.metaData()) {
                                IndexMetaData currentIndexMetaData = currentState.metaData().index(indexMetaData.index());
                                if (currentIndexMetaData == null || currentIndexMetaData.version() != indexMetaData.version()) {
                                    metaDataBuilder.put(indexMetaData, false);
                                } else {
                                    metaDataBuilder.put(currentIndexMetaData, false);
                                }
                            }
                            builder.metaData(metaDataBuilder);
                        }

                        return builder.build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                        newStateProcessed.onNewClusterStateFailed(t);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        sendInitialStateEventIfNeeded();
                        newStateProcessed.onNewClusterStateProcessed();
                    }
                });
            }
        }
    }

    /**
     * master 接受到其他节点的加入请求   更新到latestDiscoNodes
     * @param node
     * @return
     */
    private ClusterState handleJoinRequest(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticsearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
        }

        ClusterState state = clusterService.state();
        if (!transportService.addressSupported(node.address().getClass())) {
            // TODO, what should we do now? Maybe inform that node that its crap?
            logger.warn("received a wrong address type from [{}], ignoring...", node);
        } else {
            // try and connect to the node, if it fails, we can raise an exception back to the client...
            transportService.connectToNode(node);
            state = clusterService.state();

            // validate the join request, will throw a failure if it fails, which will get back to the
            // node calling the join request  发送验证请求
            membership.sendValidateJoinRequestBlocking(node, state, pingTimeout);

            clusterService.submitStateUpdateTask("zen-disco-receive(join from node[" + node + "])", Priority.URGENT, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.nodes().nodeExists(node.id())) {
                        // the node already exists in the cluster
                        logger.warn("received a join request for an existing node [{}]", node);
                        // still send a new cluster state, so it will be re published and possibly update the other node
                        return ClusterState.builder(currentState).build();
                    }
                    //将此节点添加到  DiscoveryNodes中
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder(currentState.nodes());
                    for (DiscoveryNode existingNode : currentState.nodes()) {
                        if (node.address().equals(existingNode.address())) {
                            builder.remove(existingNode.id());
                            logger.warn("received join request from node [{}], but found existing node {} with same address, removing existing node", node, existingNode);
                        }
                    }
                    latestDiscoNodes = builder.build();
                    // add the new node now (will update latestDiscoNodes on publish)
                    return ClusterState.builder(currentState).nodes(latestDiscoNodes.newNode(node)).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
        }
        return state;
    }

    /**
     * 选举 master节点   选举节点时使用的是pingService
     *
     * 故障检测  使用transport
     *
     * 根据ping 返回的response 选举master
     *
     * 若是ping响应中没有 任何master 信息，则直接根据所有节点配置  重新选举
     *
     * 也就是说优先根据ping response 中已有的master信息选举，一般来说 只有一个master，当新的节点加入时，还会选择这个master
     * 若是ping Response 中有 不同的master节点，难道是出现了网络分区
     * @return
     */
    private DiscoveryNode findMaster() {
        //ping 其他节点
        ZenPing.PingResponse[] fullPingResponses = pingService.pingAndWait(pingTimeout);
        if (fullPingResponses == null) {
            logger.trace("No full ping responses");
            return null;
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("full ping responses:");
            if (fullPingResponses.length == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : fullPingResponses) {
                    sb.append("\n\t--> ").append("target [").append(pingResponse.target()).append("], master [").append(pingResponse.master()).append("]");
                }
            }
            logger.trace(sb.toString());
        }

        // filter responses
        List<ZenPing.PingResponse> pingResponses = Lists.newArrayList();
        for (ZenPing.PingResponse pingResponse : fullPingResponses) {
            DiscoveryNode node = pingResponse.target();  // ping的目标节点
            if (masterElectionFilterClientNodes && (node.clientNode() || (!node.masterNode() && !node.dataNode()))) {
                // filter out the client node, which is a client node, or also one that is not data and not master (effectively, client)
            } else if (masterElectionFilterDataNodes && (!node.masterNode() && node.dataNode())) {
                // filter out data node that is not also master
            } else {
                pingResponses.add(pingResponse);
            }
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("filtered ping responses: (filter_client[").append(masterElectionFilterClientNodes).append("], filter_data[").append(masterElectionFilterDataNodes).append("])");
            if (pingResponses.isEmpty()) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : pingResponses) {
                    sb.append("\n\t--> ").append("target [").append(pingResponse.target()).append("], master [").append(pingResponse.master()).append("]");
                }
            }
            logger.debug(sb.toString());
        }
        //ping 得到的响应中 会告知当前节点知道的master 节点
        List<DiscoveryNode> pingMasters = newArrayList();
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            if (pingResponse.master() != null) {
                pingMasters.add(pingResponse.master());
            }
        }

        Set<DiscoveryNode> possibleMasterNodes = Sets.newHashSet();
        possibleMasterNodes.add(localNode);
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            possibleMasterNodes.add(pingResponse.target());
        }
        // if we don't have enough master nodes, we bail, even if we get a response that indicates
        // there is a master by other node, we don't see enough...
        if (!electMaster.hasEnoughMasterNodes(possibleMasterNodes)) {
            return null;
        }
        // 若是ping响应都没有master信息，则选举一个master，若是本地节点则返回，否则 返回null，不做任何处理，等待被选为master节点 去执行   集群都刚启动时会出现
        //若是从ping响应中选举的master，若是master不为null，则返回master
        if (pingMasters.isEmpty()) {
            // lets tie break between discovered nodes
            DiscoveryNode electedMaster = electMaster.electMaster(possibleMasterNodes);
            if (localNode.equals(electedMaster)) {
                return localNode;
            }
        } else {
            DiscoveryNode electedMaster = electMaster.electMaster(pingMasters);
            if (electedMaster != null) {
                return electedMaster;
            }
        }
        return null;
    }

    private ClusterState rejoin(ClusterState clusterState, String reason) {
        logger.warn(reason + ", current nodes: {}", clusterState.nodes());
        nodesFD.stop();
        masterFD.stop(reason);
        master = false;

        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(NO_MASTER_BLOCK)
                .addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                .build();

        // clear the routing table, we have no master, so we need to recreate the routing when we reform the cluster
        RoutingTable routingTable = RoutingTable.builder().build();
        // we also clean the metadata, since we are going to recover it if we become master
        MetaData metaData = MetaData.builder().build();

        // clean the nodes, we are now not connected to anybody, since we try and reform the cluster
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();

        asyncJoinCluster();

        return ClusterState.builder(clusterState)
                .blocks(clusterBlocks)
                .nodes(latestDiscoNodes)
                .routingTable(routingTable)
                .metaData(metaData)
                .build();
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    // 新的集群状态更新 时调用
    private class NewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {

        @Override
        public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
            handleNewClusterStateFromMaster(clusterState, newStateProcessed);
        }
    }


    // 处理 join leave请求
    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public ClusterState onJoin(DiscoveryNode node) {
            return handleJoinRequest(node);
        }

        @Override
        public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFailureListener implements NodesFaultDetection.Listener {

        @Override
        public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }
    }


    /**
     *
     */
    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        /**
         *  连接master 节点出现异常
         * @param masterNode
         * @param reason
         */
        @Override
        public void onMasterFailure(DiscoveryNode masterNode, String reason) {
            handleMasterGone(masterNode, reason);
        }

        /**
         * 能够连接到master，但是还没有join
         */
        @Override
        public void onDisconnectedFromMaster() {
            // got disconnected from the master, send a join request
            DiscoveryNode masterNode = latestDiscoNodes.masterNode();
            try {
                //再次发送加入请求
                membership.sendJoinRequest(masterNode, localNode);
            } catch (Exception e) {
                logger.warn("failed to send join request on disconnection from master [{}]", masterNode);
            }
        }
    }

    static class RejoinClusterRequest extends TransportRequest {

        private String fromNodeId;

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromNodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(fromNodeId);
        }
    }

    class RejoinClusterRequestHandler extends BaseTransportRequestHandler<RejoinClusterRequest> {

        static final String ACTION = "discovery/zen/rejoin";

        @Override
        public RejoinClusterRequest newInstance() {
            return new RejoinClusterRequest();
        }

        @Override
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel) throws Exception {
            clusterService.submitStateUpdateTask("received a request to rejoin the cluster from [" + request.fromNodeId + "]", Priority.URGENT, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Exception e) {
                        logger.warn("failed to send response on rejoin cluster request handling", e);
                    }
                    return rejoin(currentState, "received a request to rejoin the cluster from [" + request.fromNodeId + "]");
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int minimumMasterNodes = settings.getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES,
                    ZenDiscovery.this.electMaster.minimumMasterNodes());
            if (minimumMasterNodes != ZenDiscovery.this.electMaster.minimumMasterNodes()) {
                logger.info("updating {} from [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES,
                        ZenDiscovery.this.electMaster.minimumMasterNodes(), minimumMasterNodes);
                handleMinimumMasterNodesChanged(minimumMasterNodes);
            }
        }
    }
}
