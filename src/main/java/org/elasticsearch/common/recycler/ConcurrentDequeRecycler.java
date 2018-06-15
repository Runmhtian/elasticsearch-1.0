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

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Recycler} implementation based on a concurrent {@link Deque}. This implementation is thread-safe.
 */
public class ConcurrentDequeRecycler<T> extends DequeRecycler<T> {

    // we maintain size separately because concurrent deque implementations typically have linear-time size() impls
    final AtomicInteger size;

    public ConcurrentDequeRecycler(C<T> c, int maxSize) {
        super(c, ConcurrentCollections.<T>newDeque(), maxSize);
        this.size = new AtomicInteger();
    }

    @Override
    public void close() {
        assert deque.size() == size.get();
        super.close();
        size.set(0);
    }

    @Override
    public V<T> obtain(int sizing) {
        final V<T> v = super.obtain(sizing);
        if (v.isRecycled()) {
            size.decrementAndGet();
        }
        return v;
    }

    @Override
    protected boolean beforeRelease() {
        return size.incrementAndGet() <= maxSize;
    }//先自增1  ，然后看是否队列满（maxSize）
    //保证了  此方法返回True的时候，queue已经占据了一个位置，肯定是能够写入的，任何一个线程在自增size时，都保证 自己在能够写入queue情况下
    //写入。  size是原子类型的，不会超过maxSize

    @Override
    protected void afterRelease(boolean recycled) {
        if (!recycled) {
            size.decrementAndGet();
        }
    }

}
