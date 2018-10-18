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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 * Search type represent the manner at which the search operation is executed.
 *
 *
 */
public enum SearchType {
    /**
     * 初始化一个scatter 阶段，来计算term的频次  为了更高的准确性
     * 若是打分使用tfidf的话 ，scatter过程应该是 计算idf值，逆文档频率
     * Same as {@link #QUERY_THEN_FETCH}, except for an initial scatter phase which goes and computes the distributed
     * term frequencies for more accurate scoring.
     */
    DFS_QUERY_THEN_FETCH((byte) 0),
    /**
     * 查询所有分片，但是返回的不是文档内容，而是文档相关信息（文档id，score等），使用各个分片返回的信息进行排序，
     * 然后根据排序结果去相关的shard取document内容
     *
     * 这种 每个分片返回size个，最终排序后再取size个
     * The query is executed against all shards, but only enough information is returned (not the document content).
     * The results are then sorted and ranked, and based on it, only the relevant shards are asked for the actual
     * document content. The return number of hits is exactly as specified in size, since they are the only ones that
     * are fetched. This is very handy when the index has a lot of shards (not replicas, shard id groups).
     */
    QUERY_THEN_FETCH((byte) 1),
    /**
     * 与QUERY_AND_FETCH相比多了一个scatter阶段
     * Same as {@link #QUERY_AND_FETCH}, except for an initial scatter phase which goes and computes the distributed
     * term frequencies for more accurate scoring.
     */
    DFS_QUERY_AND_FETCH((byte) 2),
    /**
     * 查询所有分片，并取回结果，每个分片返回一定size的results，也就是各个分片分别进行排序  然会一定数量的文档  包含文档内容
     *
     * 这种查询最终返回结果是 请求size的n倍，n为number of shard   并且排序不准确
     *
     * The most naive (and possibly fastest) implementation is to simply execute the query on all relevant shards
     * and return the results. Each shard returns size results. Since each shard already returns size hits, this
     * type actually returns size times number of shards results back to the caller.
     */
    QUERY_AND_FETCH((byte) 3),
    /**
     * 不排序
     * Performs scanning of the results which executes the search without any sorting.
     * It will automatically start scrolling the result set.
     */
    SCAN((byte) 4),
    /**
     * Only counts the results, will still execute facets and the like.
     * counts类型查询
     */
    COUNT((byte) 5);

    /**
     * The default search type ({@link #QUERY_THEN_FETCH}.
     */
    public static final SearchType DEFAULT = QUERY_THEN_FETCH;

    private byte id;

    SearchType(byte id) {
        this.id = id;
    }

    /**
     * The internal id of the type.
     */
    public byte id() {
        return this.id;
    }

    /**
     * Constructs search type based on the internal id.
     */
    public static SearchType fromId(byte id) {
        if (id == 0) {
            return DFS_QUERY_THEN_FETCH;
        } else if (id == 1) {
            return QUERY_THEN_FETCH;
        } else if (id == 2) {
            return DFS_QUERY_AND_FETCH;
        } else if (id == 3) {
            return QUERY_AND_FETCH;
        } else if (id == 4) {
            return SCAN;
        } else if (id == 5) {
            return COUNT;
        } else {
            throw new ElasticsearchIllegalArgumentException("No search type for [" + id + "]");
        }
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", "query_and_fetch"/"queryAndFetch", and "scan".
     */
    public static SearchType fromString(String searchType) throws ElasticsearchIllegalArgumentException {
        if (searchType == null) {
            return SearchType.DEFAULT;
        }
        if ("dfs_query_then_fetch".equals(searchType)) {
            return SearchType.DFS_QUERY_THEN_FETCH;
        } else if ("dfs_query_and_fetch".equals(searchType)) {
            return SearchType.DFS_QUERY_AND_FETCH;
        } else if ("query_then_fetch".equals(searchType)) {
            return SearchType.QUERY_THEN_FETCH;
        } else if ("query_and_fetch".equals(searchType)) {
            return SearchType.QUERY_AND_FETCH;
        } else if ("scan".equals(searchType)) {
            return SearchType.SCAN;
        } else if ("count".equals(searchType)) {
            return SearchType.COUNT;
        } else {
            throw new ElasticsearchIllegalArgumentException("No search type for [" + searchType + "]");
        }
    }
}
