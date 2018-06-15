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

import org.elasticsearch.ElasticsearchIllegalStateException;

import java.util.Deque;

/**
 * A {@link Recycler} implementation based on a {@link Deque}. This implementation is NOT thread-safe.
 *
 * 回收再利用  T对象
 *
 * 把T对象封装  使用的时候根据obtain来获取  封装了T对象的 Recycler.V
 * 可以在使用完T对象时候，使用Recycler.V中的release 来clear数据，并放到deque中
 *
 * 下次再obtain时，若是queue没有可使用的则new一个，否则可以使用上次回收的对象   其实就是通过obtain方法来产生可用对象，这个对象可能是new的可能是回收的
 *
 * 非线程安全  意思是deque的size  调用beforeRelease  队列的size判断 得到 队列不满  可回收
 *  但是在addFirst时（若是多线程）可能已经超过了size
 *
 *  ConcurrentDequeRecycler   通过使用原子类型  控制size   而不是使用queue.size
 *
 *
 *  注意这里的线程安全是保证使用ConcurrentDequeRecycler操作时线程安全，也就是这个类是线程安全的
 *
 *  而不是  T对象的线程安全，比如用多线程操作  ConcurrentDequeRecycler.obtain().v()  对象   这是T类线程安全问题

 */
public class DequeRecycler<T> extends AbstractRecycler<T> {

    final Deque<T> deque; //队列中存的是  T对象
    final int maxSize;

    public DequeRecycler(C<T> c, Deque<T> queue, int maxSize) {  //C是一个 新建实例的对象 newInstance   T
        super(c);
        this.deque = queue;
        this.maxSize = maxSize;
    }

    @Override
    public void close() {
        deque.clear();
    }

    @Override
    public V<T> obtain(int sizing) {
        /*
        使用这个方法来  获取一个Recycler.V对象，而Recycler.V  属性含有T对象
         */
        final T v = deque.pollFirst();  //获取队列中第一个 T对象
        if (v == null) {
            return new DV(c.newInstance(sizing), false);  //若是null， new DV对象
            // 第一个参数 调用c.newInstance(sizing)来创建一个T对象   新new的recycled=false
        }
        return new DV(v, true);   //new DV   recycled=true  是v 已经回收的意思？  那就是
        //队列中存放的是  已经回收了的对象
    }

    /** Called before releasing an object, returns true if the object should be recycled and false otherwise. */
    protected boolean beforeRelease() {
        return deque.size() < maxSize;
    }

    /** Called after a release. */
    protected void afterRelease(boolean recycled) {}

    private class DV implements Recycler.V<T> {

        T value;
        final boolean recycled;  //用来判断  是否是已经回收过的对象

        DV(T value, boolean recycled) {
            this.value = value;
            this.recycled = recycled;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return recycled;
        }

        @Override
        public boolean release() {
            if (value == null) {
                throw new ElasticsearchIllegalStateException("recycler entry already released...");
            }
            final boolean recycle = beforeRelease();//若是队列大小 小于maxSize  则返回true  也就是队列还没满
            if (recycle) {
                //
                c.clear(value);  //调用C中的clear方法   这里就是清除OpenHashMap中的数据
                deque.addFirst(value);  //将其放入队列中   也就是这个对象可以循环使用？
            }
            value = null;
            afterRelease(recycle);
            return true;
        }
    }
}
