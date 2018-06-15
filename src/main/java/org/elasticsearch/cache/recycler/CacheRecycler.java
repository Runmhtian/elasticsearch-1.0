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

package org.elasticsearch.cache.recycler;

import com.carrotsearch.hppc.*;
import com.google.common.base.Strings;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Locale;

import static org.elasticsearch.common.recycler.Recyclers.*;

@SuppressWarnings("unchecked")
public class CacheRecycler extends AbstractComponent {
    public final Recycler<ObjectObjectOpenHashMap> hashMap;
    public final Recycler<ObjectOpenHashSet> hashSet;
    public final Recycler<DoubleObjectOpenHashMap> doubleObjectMap;
    public final Recycler<LongObjectOpenHashMap> longObjectMap;
    public final Recycler<LongLongOpenHashMap> longLongMap;
    public final Recycler<IntIntOpenHashMap> intIntMap;
    public final Recycler<FloatIntOpenHashMap> floatIntMap;
    public final Recycler<DoubleIntOpenHashMap> doubleIntMap;
    public final Recycler<LongIntOpenHashMap> longIntMap;
    public final Recycler<ObjectIntOpenHashMap> objectIntMap;
    public final Recycler<IntObjectOpenHashMap> intObjectMap;
    public final Recycler<ObjectFloatOpenHashMap> objectFloatMap;

    public void close() {
        hashMap.close();
        hashSet.close();
        doubleObjectMap.close();
        longObjectMap.close();
        longLongMap.close();
        intIntMap.close();
        floatIntMap.close();
        doubleIntMap.close();
        longIntMap.close();
        objectIntMap.close();
        intObjectMap.close();
        objectFloatMap.close();
    }
    /*
    type
    limit
    smart_size

     */
    @Inject
    public CacheRecycler(Settings settings) {  //Settings 从CacheRecyclerModule中注入？
        super(settings);
        final Type type = Type.parse(settings.get("type"));
        int limit = settings.getAsInt("limit", 10);
        int smartSize = settings.getAsInt("smart_size", 1024);

        //获取可用处理器的数目 processors   default Math.min(32, Runtime.getRuntime().availableProcessors())
        final int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);

        hashMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<ObjectObjectOpenHashMap>() {
            @Override
            public ObjectObjectOpenHashMap newInstance(int sizing) {
                return new ObjectObjectOpenHashMap(size(sizing));
            }

            @Override
            public void clear(ObjectObjectOpenHashMap value) {
                value.clear();
            }
        });
        hashSet = build(type, limit, smartSize, availableProcessors, new Recycler.C<ObjectOpenHashSet>() {
            @Override
            public ObjectOpenHashSet newInstance(int sizing) {
                return new ObjectOpenHashSet(size(sizing), 0.5f);
            }

            @Override
            public void clear(ObjectOpenHashSet value) {
                value.clear();
            }
        });
        doubleObjectMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<DoubleObjectOpenHashMap>() {
            @Override
            public DoubleObjectOpenHashMap newInstance(int sizing) {
                return new DoubleObjectOpenHashMap(size(sizing));
            }

            @Override
            public void clear(DoubleObjectOpenHashMap value) {
                value.clear();
            }
        });
        longObjectMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<LongObjectOpenHashMap>() {
            @Override
            public LongObjectOpenHashMap newInstance(int sizing) {
                return new LongObjectOpenHashMap(size(sizing));
            }

            @Override
            public void clear(LongObjectOpenHashMap value) {
                value.clear();
            }
        });
        longLongMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<LongLongOpenHashMap>() {
            @Override
            public LongLongOpenHashMap newInstance(int sizing) {
                return new LongLongOpenHashMap(size(sizing));
            }

            @Override
            public void clear(LongLongOpenHashMap value) {
                value.clear();
            }
        });
        intIntMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<IntIntOpenHashMap>() {
            @Override
            public IntIntOpenHashMap newInstance(int sizing) {
                return new IntIntOpenHashMap(size(sizing));
            }

            @Override
            public void clear(IntIntOpenHashMap value) {
                value.clear();
            }
        });
        floatIntMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<FloatIntOpenHashMap>() {
            @Override
            public FloatIntOpenHashMap newInstance(int sizing) {
                return new FloatIntOpenHashMap(size(sizing));
            }

            @Override
            public void clear(FloatIntOpenHashMap value) {
                value.clear();
            }
        });
        doubleIntMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<DoubleIntOpenHashMap>() {
            @Override
            public DoubleIntOpenHashMap newInstance(int sizing) {
                return new DoubleIntOpenHashMap(size(sizing));
            }

            @Override
            public void clear(DoubleIntOpenHashMap value) {
                value.clear();
            }
        });
        longIntMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<LongIntOpenHashMap>() {
            @Override
            public LongIntOpenHashMap newInstance(int sizing) {
                return new LongIntOpenHashMap(size(sizing));
            }

            @Override
            public void clear(LongIntOpenHashMap value) {
                value.clear();
            }
        });
        objectIntMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<ObjectIntOpenHashMap>() {
            @Override
            public ObjectIntOpenHashMap newInstance(int sizing) {
                return new ObjectIntOpenHashMap(size(sizing));
            }

            @Override
            public void clear(ObjectIntOpenHashMap value) {
                value.clear();
            }
        });
        intObjectMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<IntObjectOpenHashMap>() {
            @Override
            public IntObjectOpenHashMap newInstance(int sizing) {
                return new IntObjectOpenHashMap(size(sizing));
            }

            @Override
            public void clear(IntObjectOpenHashMap value) {
                value.clear();
            }
        });
        objectFloatMap = build(type, limit, smartSize, availableProcessors, new Recycler.C<ObjectFloatOpenHashMap>() {
            @Override
            public ObjectFloatOpenHashMap newInstance(int sizing) {
                return new ObjectFloatOpenHashMap(size(sizing));
            }

            @Override
            public void clear(ObjectFloatOpenHashMap value) {
                value.clear();
            }
        });
    }

    public <K, V> Recycler.V<ObjectObjectOpenHashMap<K, V>> hashMap(int sizing) {
        return (Recycler.V) hashMap.obtain(sizing);
    }

    public <T> Recycler.V<ObjectOpenHashSet<T>> hashSet(int sizing) {
        return (Recycler.V) hashSet.obtain(sizing);
    }

    public <T> Recycler.V<DoubleObjectOpenHashMap<T>> doubleObjectMap(int sizing) {
        return (Recycler.V) doubleObjectMap.obtain(sizing);
    }

    public <T> Recycler.V<LongObjectOpenHashMap<T>> longObjectMap(int sizing) {
        return (Recycler.V) longObjectMap.obtain(sizing);
    }

    public Recycler.V<LongLongOpenHashMap> longLongMap(int sizing) {
        return longLongMap.obtain(sizing);
    }

    public Recycler.V<IntIntOpenHashMap> intIntMap(int sizing) {
        return intIntMap.obtain(sizing);
    }

    public Recycler.V<FloatIntOpenHashMap> floatIntMap(int sizing) {
        return floatIntMap.obtain(sizing);
    }

    public Recycler.V<DoubleIntOpenHashMap> doubleIntMap(int sizing) {
        return doubleIntMap.obtain(sizing);
    }

    public Recycler.V<LongIntOpenHashMap> longIntMap(int sizing) {
        return longIntMap.obtain(sizing);
    }

    public <T> Recycler.V<ObjectIntOpenHashMap<T>> objectIntMap(int sizing) {
        return (Recycler.V) objectIntMap.obtain(sizing);
    }

    public <T> Recycler.V<IntObjectOpenHashMap<T>> intObjectMap(int sizing) {
        return (Recycler.V) intObjectMap.obtain(sizing);
    }

    public <T> Recycler.V<ObjectFloatOpenHashMap<T>> objectFloatMap(int sizing) {
        return (Recycler.V) objectFloatMap.obtain(sizing);
    }

    static int size(int sizing) {
        return sizing > 0 ? sizing : 256;
    }

    private <T> Recycler<T> build(Type type, int limit, int smartSize, int availableProcessors, Recycler.C<T> c) {
        Recycler<T> recycler;
        try {
            //根据类型调用对应的build方法  返回对应的Recycler
            recycler = type.build(c, limit, availableProcessors);
            if (smartSize > 0) {
                recycler = sizing(recycler, none(c), smartSize);
            }
        } catch (IllegalArgumentException ex) {
            throw new ElasticsearchIllegalArgumentException("no type support [" + type + "] for recycler");
        }

        return recycler;
    }


    /*
    枚举方法
    实质上是用内部类实现的
    重写Type中的build方法


    threadLocal
    Create a thread-local recycler, where each thread will have its own instance, create through the provided factory.
     */
    public static enum Type {
        SOFT_THREAD_LOCAL {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return threadLocal(softFactory(dequeFactory(c, limit)));
            }
        },
        THREAD_LOCAL {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return threadLocal(dequeFactory(c, limit));
            }
        },
        QUEUE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrentDeque(c, limit);
            }
        },
        SOFT_CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrent(softFactory(dequeFactory(c, limit)), availableProcessors);
            }
        },
        CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrent(dequeFactory(c, limit), availableProcessors);
            }
        },
        NONE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return none(c);
            }
        };

        public static Type parse(String type) {
            if (Strings.isNullOrEmpty(type)) {
                return SOFT_CONCURRENT;
            }
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new ElasticsearchIllegalArgumentException("no type support [" + type + "]");
            }
        }

        abstract <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors);
    }
}