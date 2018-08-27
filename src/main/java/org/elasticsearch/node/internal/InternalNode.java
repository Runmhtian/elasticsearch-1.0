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

package org.elasticsearch.node.internal;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.bulk.udp.BulkUdpModule;
import org.elasticsearch.bulk.udp.BulkUdpService;
import org.elasticsearch.cache.NodeCache;
import org.elasticsearch.cache.NodeCacheModule;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecyclerModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClientModule;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.CachedStreams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironmentModule;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerModule;
import org.elasticsearch.index.search.shape.ShapeModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.monitor.MonitorModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.percolator.PercolatorModule;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversManager;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.tribe.TribeModule;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.watcher.ResourceWatcherModule;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class InternalNode implements Node {

    private final Lifecycle lifecycle = new Lifecycle();

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;

    private final PluginsService pluginsService;

    private final Client client;

    public InternalNode() throws ElasticsearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    public InternalNode(Settings pSettings, boolean loadConfigSettings) throws ElasticsearchException {
        // pSettings  配置文件  和loadConfigSettings=false  调用prepareSettings  新得到一个  tuple
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(pSettings, loadConfigSettings);
        //根据settings 添加tribe node 配置
        tuple = new Tuple<Settings, Environment>(TribeService.processSettings(tuple.v1()), tuple.v2());

        Version version = Version.CURRENT;

        ESLogger logger = Loggers.getLogger(Node.class, tuple.v1().get("name"));
        logger.info("version[{}], pid[{}], build[{}/{}]", version, JvmInfo.jvmInfo().pid(), Build.CURRENT.hashShort(), Build.CURRENT.timestamp());

        logger.info("initializing ...");

        if (logger.isDebugEnabled()) {
            Environment env = tuple.v2();
            logger.debug("using home [{}], config [{}], data [{}], logs [{}], work [{}], plugins [{}]",
                    env.homeFile(), env.configFile(), Arrays.toString(env.dataFiles()), env.logsFile(),
                    env.workFile(), env.pluginsFile());
        }

        this.pluginsService = new PluginsService(tuple.v1(), tuple.v2());  //new pluginsService  setting 中plugin.types
        this.settings = pluginsService.updatedSettings();  //更新settings  来源于plugin中的additionalSettings
        // create the environment based on the finalized (processed) view of the settings
        this.environment = new Environment(this.settings());

        /*
        初始化压缩对象，使用配置  "compress.lzf.decoder"   "compress.default.type"
         */
        CompressorFactory.configure(settings);

        //node environment
        /*
        根据 path.data  测试是否能够获取文件锁
        初始化这个几个参数
        nodeFiles                      {path.data}
        nodeIndicesLocations            {path.data}/indices
        locks                           {path.data}/nodes/0/node.lock               0 是localNodeId   文件锁
        localNodeId                     0                                        本地node的id  一个机器可能有多个nodeEnvironment
         */
        NodeEnvironment nodeEnvironment = new NodeEnvironment(this.settings, this.environment);
        /*
        es 使用google开源的依赖注入框架guice，直接把guice源码放入到了es源码下，使用ModulesBuilder对guice中的module和injector进行简单封装
        用于构建es的模块

         */
        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new Version.Module(version));  //new Version中的静态内部类  Module   版本模块
        //根据setting Type配置   初始化CacheRecycler   不同配置
        /*
        Recycler  封装了各种map 如 IntIntOpenHashMap  LongObjectOpenHashMap
        Recycler的主要功能是，通过队列使的这些map对象可以回收并新利用   NoneRecycler  没有重用机制

        可选择的在Recycler 外层又包了一层  引用方式   threadLocal cocurrent    softreference

        避免多个地方使用集合需要进行多次初始化来申请内存。并且减少垃圾回收。
         */
        modules.add(new CacheRecyclerModule(settings));
        /*
        初始化了多个Recycler，Recycler中封装了各中数据，byte[]，int[]，long[]，double[]，Object[]，同样使用重用机制，
        并且加入了threadLocal，cocurrent，softreference，根据配置，Recycler可以有不同的策略。避免多个地方使用集合需要进行多次初始化来申请内存。并且减少垃圾回收。
         */

        modules.add(new PageCacheRecyclerModule(settings));
        // 将实例pluginsService  绑定到 PluginsService.class
        modules.add(new PluginsModule(settings, pluginsService));
        //配置 模块
        modules.add(new SettingsModule(settings));

        /*
        // 节点 模块
        bind(Node.class).toInstance(node);   接口绑定到实现类
        bind(NodeSettingsService.class).asEagerSingleton();   节点配置 更新集群配置
        bind(NodeService.class).asEagerSingleton();  节点服务
        public NodeService(Settings settings, ThreadPool threadPool, MonitorService monitorService, Discovery discovery,
                       TransportService transportService, IndicesService indicesService,
                       PluginsService pluginService, CircuitBreakerService circuitBreakerService, Version version)
        参数依赖注入
        NodeService  将各种服务都涵盖进来
         */
        modules.add(new NodeModule(this));
        /*
        NetworkService  用于获取InetAddress对象
         */
        modules.add(new NetworkModule());
        // 节点缓存 bind(NodeCache.class).asEagerSingleton();
//        bind(ByteBufferCache.class).asEagerSingleton();
        modules.add(new NodeCacheModule(settings));
        /*
        主要是两者的初始化
        MvelScriptEngineService   mvel是基于java的表达式语言

        script.native.?.type 中的配置
        NativeScriptEngineService
         */
        modules.add(new ScriptModule(settings));
        //bind(Environment.class).toInstance(environment)
        modules.add(new EnvironmentModule(environment));
        // 节点点环境信息
        modules.add(new NodeEnvironmentModule(nodeEnvironment));
        // 集群名称
        modules.add(new ClusterNameModule(settings));
        /*
                public static final String SAME = "same";
        public static final String GENERIC = "generic";
        public static final String GET = "get";
        public static final String INDEX = "index";
        public static final String BULK = "bulk";
        public static final String SEARCH = "search";
        public static final String SUGGEST = "suggest";
        public static final String PERCOLATE = "percolate";
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String MERGE = "merge";
        public static final String REFRESH = "refresh";
        public static final String WARMER = "warmer";
        public static final String SNAPSHOT = "snapshot";
        public static final String OPTIMIZE = "optimize";

        初始化 这些值 对应的线程池
         */
        modules.add(new ThreadPoolModule(settings));
        /*
        DiscoveryService  参数注入 Discovery
         */
        modules.add(new DiscoveryModule(settings));
        // 集群相关service
        modules.add(new ClusterModule(settings));
        // restcontroller    RestActionModule   定义了restful  api
        modules.add(new RestModule(settings));

        //节点通信模块
        modules.add(new TransportModule(settings));
        //跟transport类似，使用http进行通信。
        if (settings.getAsBoolean("http.enabled", true)) {
            modules.add(new HttpServerModule(settings));
        }
        //es的river服务，用于同步其他数据源的数据到es，用户可以自定义river，通过RiversModule进行注册。我们看一下river接口。
        modules.add(new RiversModule(settings));
        //索引模块，初始化索引相关服务，如IndicesClusterStateService，IndicesTermsFilterCache，IndicesFieldDataCache，IndicesStore，IndexingMemoryController等。
        //
        modules.add(new IndicesModule(settings));
        //查询模块，初始化查询相关的服务
        modules.add(new SearchModule());
        //elasticsearch中的绝大部分操作都是通过相应的action，这些action在action包中。
        modules.add(new ActionModule(false));
        //es的各种监控服务，比如network，jvm，fs等。
        modules.add(new MonitorModule(settings));
        //gateway 模块负责当集群 full restart 时的元信息(state)数据恢复.  http://www.easyice.cn/archives/226
        modules.add(new GatewayModule(settings));
        //es客户端模块。
        modules.add(new NodeClientModule());
        //udp bulk  The Bulk UDP services has been removed. Use the standard Bulk API instead.   version 6.3
        modules.add(new BulkUdpModule());
        // ShapeFetchService  从index type id 解析出shape
        modules.add(new ShapeModule());

        //es的普通查询是通过某些条件来查询满足的文档，percolator则不同，先是注册一些条件，然后查询一条文档是否满足其中的某些条件。

//        es的percolator特性在数据分类、数据路由、事件监控和预警方面都有很好的应用。
// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-percolate-query.html#_sample_usage
        modules.add(new PercolatorModule());
        //资源监控模块，其他es服务需要监控的信息可以注册到资源监控服务。ResourceWatcherService定期的回去这些注册的类中的接口方法，默认间隔60s。
        modules.add(new ResourceWatcherModule());
        //Snapshot repository模块。负责索引或者集群级别的操作，仅仅能够在master上调用。
        modules.add(new RepositoriesModule());
        //https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-tribe.html   在5.4.0中弃用
        modules.add(new TribeModule());

        injector = modules.createInjector();

        client = injector.getInstance(Client.class);

        logger.info("initialized");
    }

    @Override
    public Settings settings() {
        return this.settings;
    }

    @Override
    public Client client() {
        return client;
    }

    public Node start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("starting ...");

        // hack around dependency injection problem (for now...)
        injector.getInstance(Discovery.class).setAllocationService(injector.getInstance(AllocationService.class));

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).start();
        }

        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndexingMemoryController.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(RiversManager.class).start();
        injector.getInstance(ClusterService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();
        injector.getInstance(TransportService.class).start();
        DiscoveryService discoService = injector.getInstance(DiscoveryService.class).start();

        // gateway should start after disco, so it can try and recovery from gateway on "start"
        injector.getInstance(GatewayService.class).start();

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        }
        injector.getInstance(BulkUdpService.class).start();
        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(TribeService.class).start();

        logger.info("started");

        return this;
    }

    @Override
    public Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("stopping ...");

        injector.getInstance(TribeService.class).stop();
        injector.getInstance(BulkUdpService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).stop();
        }

        injector.getInstance(RiversManager.class).stop();

        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(IndexingMemoryController.class).stop();
        injector.getInstance(IndicesTTLService.class).stop();
        injector.getInstance(IndicesService.class).stop();
        // sleep a bit to let operations finish with indices service
//        try {
//            Thread.sleep(500);
//        } catch (InterruptedException e) {
//            // ignore
//        }
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(DiscoveryService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(RestController.class).stop();
        injector.getInstance(TransportService.class).stop();

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).stop();
        }

        logger.info("stopped");

        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("closing ...");

        StopWatch stopWatch = new StopWatch("node_close");
        stopWatch.start("tribe");
        injector.getInstance(TribeService.class).close();
        stopWatch.stop().start("bulk.udp");
        injector.getInstance(BulkUdpService.class).close();
        stopWatch.stop().start("http");
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).close();
        }

        stopWatch.stop().start("rivers");
        injector.getInstance(RiversManager.class).close();

        stopWatch.stop().start("client");
        injector.getInstance(Client.class).close();
        stopWatch.stop().start("indices_cluster");
        injector.getInstance(IndicesClusterStateService.class).close();
        stopWatch.stop().start("indices");
        injector.getInstance(IndicesFilterCache.class).close();
        injector.getInstance(IndicesFieldDataCache.class).close();
        injector.getInstance(IndexingMemoryController.class).close();
        injector.getInstance(IndicesTTLService.class).close();
        injector.getInstance(IndicesService.class).close();
        stopWatch.stop().start("routing");
        injector.getInstance(RoutingService.class).close();
        stopWatch.stop().start("cluster");
        injector.getInstance(ClusterService.class).close();
        stopWatch.stop().start("discovery");
        injector.getInstance(DiscoveryService.class).close();
        stopWatch.stop().start("monitor");
        injector.getInstance(MonitorService.class).close();
        stopWatch.stop().start("gateway");
        injector.getInstance(GatewayService.class).close();
        stopWatch.stop().start("search");
        injector.getInstance(SearchService.class).close();
        stopWatch.stop().start("rest");
        injector.getInstance(RestController.class).close();
        stopWatch.stop().start("transport");
        injector.getInstance(TransportService.class).close();
        stopWatch.stop().start("percolator_service");
        injector.getInstance(PercolatorService.class).close();

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            stopWatch.stop().start("plugin(" + plugin.getName() + ")");
            injector.getInstance(plugin).close();
        }

        stopWatch.stop().start("node_cache");
        injector.getInstance(NodeCache.class).close();

        stopWatch.stop().start("script");
        injector.getInstance(ScriptService.class).close();

        stopWatch.stop().start("thread_pool");
        injector.getInstance(ThreadPool.class).shutdown();
        try {
            injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        stopWatch.stop().start("thread_pool_force_shutdown");
        try {
            injector.getInstance(ThreadPool.class).shutdownNow();
        } catch (Exception e) {
            // ignore
        }
        stopWatch.stop();

        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }

        injector.getInstance(NodeEnvironment.class).close();
        injector.getInstance(CacheRecycler.class).close();
        injector.getInstance(PageCacheRecycler.class).close();
        Injectors.close(injector);

        CachedStreams.clear();

        logger.info("closed");
    }

    @Override
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    public static void main(String[] args) throws Exception {
        final InternalNode node = new InternalNode();
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.close();
            }
        });
    }
}