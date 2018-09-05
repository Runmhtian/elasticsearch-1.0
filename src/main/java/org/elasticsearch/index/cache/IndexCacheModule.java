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

package org.elasticsearch.index.cache;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.docset.DocSetCacheModule;
import org.elasticsearch.index.cache.filter.FilterCacheModule;
import org.elasticsearch.index.cache.id.IdCacheModule;
import org.elasticsearch.index.cache.query.parser.QueryParserCacheModule;

/**
 *
 */
public class IndexCacheModule extends AbstractModule {

    private final Settings settings;

    public IndexCacheModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        //在lucene 的filter 上添加cache  这里是 过滤查询的缓存实现？  key为FilterCacheKey 包含 indexReader 和 Filter 对象
        new FilterCacheModule(settings).configure(binder());
        //使用父子文档时 id cache，simple实现  ConcurrentMap<Object, SimpleIdReaderCache>， cache的数据实际上是在 SimpleIdReaderTypeCache中
        new IdCacheModule(settings).configure(binder());
        //查询解析缓存，避免在不同的分片上解析相同的查询  使用的是guava的Cache  key为查询string，value是Query对象
        new QueryParserCacheModule(settings).configure(binder());
        //ContextDocIdSet缓存   避免多次new FixedBitSet   也就是FixedBitSet重用  位数组   缓存key是  indexReader
        new DocSetCacheModule(settings).configure(binder());

        // 将以上几个cache 包含在此类中
        bind(IndexCache.class).asEagerSingleton();
    }
}
