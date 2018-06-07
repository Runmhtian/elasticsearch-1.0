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

package org.elasticsearch.env;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class NodeEnvironment extends AbstractComponent {

    private final File[] nodeFiles;
    private final File[] nodeIndicesLocations;

    private final Lock[] locks;

    private final int localNodeId;

    @Inject
    public NodeEnvironment(Settings settings, Environment environment) {
        super(settings);

        if (!DiscoveryNode.nodeRequiresLocalStorage(settings)) {//node.client true 或者（node.data false 且 node.master false）
            nodeFiles = null;
            nodeIndicesLocations = null;
            locks = null;
            localNodeId = -1;
            return;
        }
        //dataWithClusterFiles 与 path.data配置有关系  path.data配置多个，nodesfiles就会产生多个，一般就配置一个目录（挂载多个时通过配置这个来扩展存储资源？）
        File[] nodesFiles = new File[environment.dataWithClusterFiles().length];
        Lock[] locks = new Lock[environment.dataWithClusterFiles().length];
        int localNodeId = -1;
        IOException lastException = null;

        /*
        node.max_local_storage_nodes  此参数用来配置  若是使用一台机器运行多个实例  多个实例共享path.data  最多共享个数
        https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-node.html
         */
        int maxLocalStorageNodes = settings.getAsInt("node.max_local_storage_nodes", 50);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                //{path.data}/nodes/0
                File dir = new File(new File(environment.dataWithClusterFiles()[dirIndex], "nodes"), Integer.toString(possibleLockId));
                if (!dir.exists()) {
                    FileSystemUtils.mkdirs(dir);  //创建目录
                }
                logger.trace("obtaining node lock on {} ...", dir.getAbsolutePath());
                try {
                    /*
                       NativeFSLock  中static HashSet<String> LOCK_HELD
                       中存储的是 已成功持有的文件锁名称   可以通过lockPrefix来对同一个dir产生多个锁   这里LOCK_HELD存的实际上是
                       {path.data}/nodes/0/node.lock

                        这里的tmpLock.obtain();方法实际上就是  测试当前possibleLockId是否已经在LOCK_HELD存在，
                        若是存在，说明有别的实例使用了possibleLockId，循环使用下一个
                        若是不存在
                            测试能否channel.tryLock();  来获取文件锁，文件就是{path.data}/nodes/{possibleLockId}/node.lock
                            若是获取成功则将{path.data}/nodes/0/node.lock添加到LOCK_HELD，
                            失败则从LOCK_HELD remove


                     实际上使本地运行的多个NodeEnvironment都持有一个不同的文件锁                  此通道的文件的独占锁定。

                     */
                    NativeFSLockFactory lockFactory = new NativeFSLockFactory(dir);
                    Lock tmpLock = lockFactory.makeLock("node.lock");  //返回一个  NativeFSLock对象
                    boolean obtained = tmpLock.obtain();
                    if (obtained) {
                        locks[dirIndex] = tmpLock;
                        nodesFiles[dirIndex] = dir;
                        localNodeId = possibleLockId;
                    } else {
                        logger.trace("failed to obtain node lock on {}", dir.getAbsolutePath());
                        // release all the ones that were obtained up until now
                        //只要有个没获取成功，就释放掉所有锁
                        for (int i = 0; i < locks.length; i++) {
                            if (locks[i] != null) {
                                try {
                                    locks[i].release();
                                } catch (Exception e1) {
                                    // ignore
                                }
                            }
                            locks[i] = null;
                        }
                        break;
                    }
                } catch (IOException e) {
                    logger.trace("failed to obtain node lock on {}", e, dir.getAbsolutePath());
                    lastException = new IOException("failed to obtain lock on " + dir.getAbsolutePath(), e);
                    // release all the ones that were obtained up until now
                    for (int i = 0; i < locks.length; i++) {
                        if (locks[i] != null) {
                            try {
                                locks[i].release();
                            } catch (Exception e1) {
                                // ignore
                            }
                        }
                        locks[i] = null;
                    }
                    break;
                }
            }
            if (locks[0] != null) { //说明内部循环 锁获取 obtain都成功了  直接break
                // we found a lock, break
                break;
            }
        }
        if (locks[0] == null) {
            throw new ElasticsearchIllegalStateException("Failed to obtain node lock, is the following location writable?: " + Arrays.toString(environment.dataWithClusterFiles()), lastException);
        }

        this.localNodeId = localNodeId;
        this.locks = locks;
        this.nodeFiles = nodesFiles;
        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}], local_node_id [{}]", nodesFiles, localNodeId);
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("node data locations details:\n");
            for (File file : nodesFiles) {
                sb.append(" -> ").append(file.getAbsolutePath()).append(", free_space [").append(new ByteSizeValue(file.getFreeSpace())).append("], usable_space [").append(new ByteSizeValue(file.getUsableSpace())).append("]\n");
            }
            logger.trace(sb.toString());
        }

        this.nodeIndicesLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            //   {path.data}/indices
            nodeIndicesLocations[i] = new File(nodeFiles[i], "indices");
        }
    }

    public int localNodeId() {
        return this.localNodeId;
    }

    public boolean hasNodeFile() {
        return nodeFiles != null && locks != null;
    }

    public File[] nodeDataLocations() {
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        return nodeFiles;
    }

    public File[] indicesLocations() {
        return nodeIndicesLocations;
    }

    public File[] indexLocations(Index index) {
        File[] indexLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            indexLocations[i] = new File(new File(nodeFiles[i], "indices"), index.name());
        }
        return indexLocations;
    }

    public File[] shardLocations(ShardId shardId) {
        File[] shardLocations = new File[nodeFiles.length];
        for (int i = 0; i < nodeFiles.length; i++) {
            shardLocations[i] = new File(new File(new File(nodeFiles[i], "indices"), shardId.index().name()), Integer.toString(shardId.id()));
        }
        return shardLocations;
    }

    public Set<String> findAllIndices() throws Exception {
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        Set<String> indices = Sets.newHashSet();
        for (File indicesLocation : nodeIndicesLocations) {
            File[] indicesList = indicesLocation.listFiles();
            if (indicesList == null) {
                continue;
            }
            for (File indexLocation : indicesList) {
                if (indexLocation.isDirectory()) {
                    indices.add(indexLocation.getName());
                }
            }
        }
        return indices;
    }

    public Set<ShardId> findAllShardIds() throws Exception {
        if (nodeFiles == null || locks == null) {
            throw new ElasticsearchIllegalStateException("node is not configured to store local location");
        }
        Set<ShardId> shardIds = Sets.newHashSet();
        for (File indicesLocation : nodeIndicesLocations) {
            File[] indicesList = indicesLocation.listFiles();
            if (indicesList == null) {
                continue;
            }
            for (File indexLocation : indicesList) {
                if (!indexLocation.isDirectory()) {
                    continue;
                }
                String indexName = indexLocation.getName();
                File[] shardsList = indexLocation.listFiles();
                if (shardsList == null) {
                    continue;
                }
                for (File shardLocation : shardsList) {
                    if (!shardLocation.isDirectory()) {
                        continue;
                    }
                    Integer shardId = Ints.tryParse(shardLocation.getName());
                    if (shardId != null) {
                        shardIds.add(new ShardId(indexName, shardId));
                    }
                }
            }
        }
        return shardIds;
    }

    public void close() {
        if (locks != null) {
            for (Lock lock : locks) {
                try {
                    logger.trace("releasing lock [{}]", lock);
                    lock.release();
                } catch (IOException e) {
                    logger.trace("failed to release lock [{}]", e, lock);
                }
            }
        }
    }
}
