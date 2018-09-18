package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Java's builtin ThreadLocal has a serious flaw:
 *  it can take an arbitrarily long amount of time to
 *  dereference the things you had stored in it, even once the
 *  ThreadLocal instance itself is no longer referenced.
 *  This is because there is single, master map stored for
 *  each thread, which all ThreadLocals share, and that
 *  master map only periodically purges "stale" entries.
 *
 *  While not technically a memory leak, because eventually
 *  the memory will be reclaimed, it can take a long time
 *  and you can easily hit OutOfMemoryError because from the
 *  GC's standpoint the stale entries are not reclaimable.
 * 
 *  This class works around that, by only enrolling
 *  WeakReference values into the ThreadLocal, and
 *  separately holding a hard reference to each stored
 *  value.  When you call {@link #close}, these hard
 *  references are cleared and then GC is freely able to
 *  reclaim space by objects stored in it.
 *
 *  We can not rely on {@link ThreadLocal#remove()} as it
 *  only removes the value for the caller thread, whereas
 *  {@link #close} takes care of all
 *  threads.  You should not call {@link #close} until all
 *  threads are done using the instance.
 *
 * @lucene.internal
 */
/*
ThreadLocal是一个能更够实现线程内数据共享的类

  在threadLocalMap中
    static class Entry extends WeakReference<ThreadLocal<?>> {

            Object value;

                    Entry(ThreadLocal<?> k, Object v) {
                    super(k);
                    value = v;
                    }
                    }

  弱引用机制实际上只是  对key使用，也就是ThreadLocal对象

是回收ThreadLocal对象，而非整个Entry，所以线程变量中的值T对象还是在内存中存在的

ThreadLocalMap会定期清理内部的无效Entry对象，触发的条件就是对TrheadLocal执行 set，get，remove()等操作时会触发，但是线程map中持有的是threadLocal的弱引用
当发生gc时，如果一个ThreadLocal没有外部强引用引用他，此threadlocal势必会被回收（所有线程中持有的都是弱引用），ThreadLocalMap中就会出现key为null的Entry，就没有办法访问这些key为null的Entry的value，
如果当前线程对象一直存在的话，这些key为null的Entry的value就会一直存在一条强引用链（Thread.threadLocals），虽然线程会清理内部无效的entry对象，但是只有在执行操作时才进行清除。
所以很多情况下需要使用者手动调用ThreadLocal的remove函数，手动删除不再需要的ThreadLocal，防止内存泄露。所以JDK建议将ThreadLocal变量定义成private static的，
这样的话ThreadLocal的生命周期就更长，由于一直存在ThreadLocal的强引用，所以ThreadLocal也就不会被回收，也就能保证任何时候都能根据ThreadLocal的弱引用访问到Entry的value值，然后remove它，防止内存泄露。

并且remove只移除了当前线程对象map中的threadlocal为null的记录

(当threadlocal 没有强引用的时候，意味着 threadlocal不再使用，而各个线程含有 此threadlocal为key，value的强引用，这样value不能被回收
（可能需要被回收，除了此强引用外，没有别的强引用，当然也可能线程中其他地方正在使用value，持有value的强引用，这时候value是不需要被回收的），造成内存泄露)

在使用线程池时要特别注意threadLocal的使用


CloseableThreadLocal在内部维护了一个ThreadLocal，当执行CloseableThreadLocal.set(T)时，内部其实只是代理的把值赋给内部的ThreadLocal对象，
即执行ThreadLocal.set(new WeakReference(T))。看到这里应该明白了，这里不是直接存储T，则是包装成弱引用对象，目的就是当内存不足时，jvm可以回收此对象。
会引入一个新的问题，即当前线程还存活着的时候，线程中可能需要使用此弱引用对象，因为gc而回收了弱引用对象，这样造成线程执行的问题
所以CloseableThreadLocal在内部还创建了一个WeakHashMap<Thread, T>，当线程只要存活时，则T就至少有一个强引用存在，所以不会被提前回收。
但是又引入的第2个问题，对WeakHashMap的操作要做同步synchronized限制。

（WeakHashMap 包含了对 Thread和T的强引用，因此两者都不会被回收，只有在执行get set 或者close方法时（执行get set并不是每次都执行，还有一个PURGE_MULTIPLIER来控制，
为了避免每次执行都需要遍历WeakHashMap，提供效率？？？） 来判断线程是否存活，进行垃圾回收）
 */
public class CloseableThreadLocal<T> implements Closeable {

  private ThreadLocal<WeakReference<T>> t = new ThreadLocal<>();  //value也是弱引用

  // Use a WeakHashMap so that if a Thread exits and is
  // GC'able, its entry may be removed:   value的强引用，thread的若引用   这样的话只有thread结束 value才能被回收
  private Map<Thread,T> hardRefs = new WeakHashMap<>();  // key 是weak  也就是线程存活（key不会被gc），value一定存在
  
  // Increase this to decrease frequency of purging in get:
  private static int PURGE_MULTIPLIER = 20;

  // On each get or set we decrement this; when it hits 0 we
  // purge.  After purge, we set this to
  // PURGE_MULTIPLIER * stillAliveCount.  This keeps
  // amortized cost of purging linear.
  private final AtomicInteger countUntilPurge = new AtomicInteger(PURGE_MULTIPLIER);

  protected T initialValue() {
    return null;
  }
  
  public T get() {
    WeakReference<T> weakRef = t.get();
    if (weakRef == null) {
      T iv = initialValue();
      if (iv != null) {
        set(iv);
        return iv;
      } else {
        return null;
      }
    } else {
      maybePurge();
      return weakRef.get();
    }
  }

  public void set(T object) {

    t.set(new WeakReference<>(object));

    synchronized(hardRefs) {
      hardRefs.put(Thread.currentThread(), object);
      maybePurge();
    }
  }

  private void maybePurge() {
    if (countUntilPurge.getAndDecrement() == 0) {
      purge();
    }
  }

  // Purge dead threads
  private void purge() {
    synchronized(hardRefs) {
      int stillAliveCount = 0;
      for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext();) {
        final Thread t = it.next();
        if (!t.isAlive()) {
          it.remove();
        } else {
          stillAliveCount++;
        }
      }
      int nextCount = (1+stillAliveCount) * PURGE_MULTIPLIER;
      if (nextCount <= 0) {
        // defensive: int overflow!
        nextCount = 1000000;
      }
      
      countUntilPurge.set(nextCount);
    }
  }

  @Override
  public void close() {
    // Clear the hard refs; then, the only remaining refs to
    // all values we were storing are weak (unless somewhere
    // else is still using them) and so GC may reclaim them:
    hardRefs = null;
    // Take care of the current thread right now; others will be
    // taken care of via the WeakReferences.
    if (t != null) {
      t.remove();
    }
    t = null;
  }
}
