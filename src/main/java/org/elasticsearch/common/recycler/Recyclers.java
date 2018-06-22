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

package org.elasticsearch.common.recycler;

import com.carrotsearch.hppc.hash.MurmurHash3;
import com.google.common.collect.Queues;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.lang.ref.SoftReference;

public enum Recyclers {
    ;

    /** Return a {@link Recycler} that never recycles entries. */
    public static <T> Recycler<T> none(Recycler.C<T> c) {
        return new NoneRecycler<T>(c);
    }

    /** Return a concurrent recycler based on a deque. */
    public static <T> Recycler<T> concurrentDeque(Recycler.C<T> c, int limit) {
        return new ConcurrentDequeRecycler<T>(c, limit);
    }

    /** Return a recycler based on a deque. */
    public static <T> Recycler<T> deque(Recycler.C<T> c, int limit) {
        return new DequeRecycler<T>(c, Queues.<T>newArrayDeque(), limit);
    }

    /** Return a recycler based on a deque. */
    public static <T> Recycler.Factory<T> dequeFactory(final Recycler.C<T> c, final int limit) {
        return new Recycler.Factory<T>() {
            @Override
            public Recycler<T> build() {
                return deque(c, limit);
            }
        };
    }

    /** Wrap two recyclers and forward to calls to <code>smallObjectRecycler</code> when <code>size &lt; minSize</code> and to
     *  <code>defaultRecycler</code> otherwise. */
    public static <T> Recycler<T> sizing(final Recycler<T> defaultRecycler, final Recycler<T> smallObjectRecycler, final int minSize) {
        return new FilterRecycler<T>() {

            @Override
            protected Recycler<T> getDelegate() {
                return defaultRecycler;
            }  //获取默认Recycler对象，也就是sizing  第一个参数
            //根据接口定义  使用obtain()方法时（这里没有重写obtain()）  返回这个

            @Override
            public Recycler.V<T> obtain(int sizing) {
                if (sizing > 0 && sizing < minSize) {   //sizing < minSize,minSize就是smart_size
                    return smallObjectRecycler.obtain(sizing);//  返回无回收机制的  Recycler
                }
                return super.obtain(sizing);
            }

            @Override
            public void close() {
                defaultRecycler.close();
                smallObjectRecycler.close();
            }

        };
    }

    /** Create a thread-local recycler, where each thread will have its own instance, create through the provided factory. */
    /*
    将Recycler  包装到CloseableThreadLocal   使Recycler  变为弱引用  new WeakReference<T>(object)

    每个thread都有自己的  一个WeakReference<T>副本

    CloseableThreadLocal  对java自带的ThreadLocal 进行了封装

    ThreadLocalMap  中  key 和value  都是弱引用

     */
    public static <T> Recycler<T> threadLocal(final Recycler.Factory<T> factory) {
        return new FilterRecycler<T>() {

            private final CloseableThreadLocal<Recycler<T>> recyclers;

            {
                recyclers = new CloseableThreadLocal<Recycler<T>>() {
                    @Override
                    protected Recycler<T> initialValue() {
                        return factory.build();
                    }
                };
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recyclers.get();
            }

            @Override
            public void close() {
                recyclers.close();
            }

        };
    }
    /*
    软引用
     */
    /** Create a recycler that is wrapped inside a soft reference, so that it cannot cause {@link OutOfMemoryError}s. */
    public static <T> Recycler<T> soft(final Recycler.Factory<T> factory) {
        return new FilterRecycler<T>() {

            SoftReference<Recycler<T>> ref;

            {
                ref = new SoftReference<Recycler<T>>(null);
            }

            @Override
            protected Recycler<T> getDelegate() {
                /*
                软引用
                ref.get()  判断引用对象是否已经回收

                 */
                Recycler<T> recycler = ref.get();
                if (recycler == null) {
                    recycler = factory.build();
                    ref = new SoftReference<Recycler<T>>(recycler);
                }
                return recycler;
            }

        };
    }

    /** Create a recycler that wraps data in a SoftReference.
     *  @see #soft(org.elasticsearch.common.recycler.Recycler.Factory) */
    public static <T> Recycler.Factory<T> softFactory(final Recycler.Factory<T> factory) {
        return new Recycler.Factory<T>() {
            @Override
            public Recycler<T> build() {
                return soft(factory);
            }
        };
    }

    /** Wrap the provided recycler so that calls to {@link Recycler#obtain()} and {@link Recycler.V#release()} are protected by
     *  a lock. */
    public static <T> Recycler<T> locked(final Recycler<T> recycler) {
        return new FilterRecycler<T>() {

            private final Object lock;

            {
                this.lock = new Object();
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recycler;
            }

            @Override
            public org.elasticsearch.common.recycler.Recycler.V<T> obtain(int sizing) {
                synchronized (lock) {
                    return super.obtain(sizing);
                }
            }

            @Override
            public org.elasticsearch.common.recycler.Recycler.V<T> obtain() {
                synchronized (lock) {
                    return super.obtain();
                }
            }

            @Override
            protected Recycler.V<T> wrap(final Recycler.V<T> delegate) {
                return new Recycler.V<T>() {

                    @Override
                    public boolean release() throws ElasticsearchException {
                        synchronized (lock) {
                            return delegate.release();
                        }
                    }

                    @Override
                    public T v() {
                        return delegate.v();
                    }

                    @Override
                    public boolean isRecycled() {
                        return delegate.isRecycled();
                    }

                };
            }

        };
    }

    /** Create a concurrent implementation that can support concurrent access from <code>concurrencyLevel</code> threads with little contention. */
    /*
    concurrencyLevel   可理解为并发级别
     */
    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory, final int concurrencyLevel) {
        if (concurrencyLevel < 1) {
            throw new ElasticsearchIllegalArgumentException("concurrencyLevel must be >= 1");
        }
        if (concurrencyLevel == 1) {
            return locked(factory.build());
        }
        return new FilterRecycler<T>() {

            private final Recycler<T>[] recyclers;
            {
                @SuppressWarnings("unchecked")
                final Recycler<T>[] recyclers = new Recycler[concurrencyLevel];
                this.recyclers = recyclers;
                for (int i = 0; i < concurrencyLevel; ++i) {
                    recyclers[i] = locked(factory.build());
                }
            }

            final int slot() {  //根据线程id  来产生 下标
                final long id = Thread.currentThread().getId();
                // don't trust Thread.hashCode to have equiprobable low bits
                int slot = (int) MurmurHash3.hash(id);
                // make positive, otherwise % may return negative numbers
                slot &= 0x7FFFFFFF;
                slot %= concurrencyLevel;
                return slot;
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recyclers[slot()];
            }

            @Override
            public void close() {
                for (Recycler<T> recycler : recyclers) {
                    recycler.close();
                }
            }

        };
    }

    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory) {
        return concurrent(factory, Runtime.getRuntime().availableProcessors());
    }
}
