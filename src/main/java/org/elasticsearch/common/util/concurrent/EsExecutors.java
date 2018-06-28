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

package org.elasticsearch.common.util.concurrent;

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class EsExecutors {

    /**
     * Returns the number of processors available but at most <tt>32</tt>.
     */
    public static int boundedNumberOfProcessors(Settings settings) {
        /* This relates to issues where machines with large number of cores
         * ie. >= 48 create too many threads and run into OOM see #3478
         * We just use an 32 core upper-bound here to not stress the system
         * too much with too many created threads */
        return settings.getAsInt("processors", Math.min(32, Runtime.getRuntime().availableProcessors()));
    }

    public static PrioritizedEsThreadPoolExecutor newSinglePrioritizing(ThreadFactory threadFactory) {
        return new PrioritizedEsThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory);
    }
    //scaling
    /*
        min max 通过配置来获取    wordQueue使用ExecutorScalingQueue
        handler（因为已达到线程边界和队列容量，新的执行被阻塞时使用的处理程序）  使用ForceQueuePolicy
     */
    public static EsThreadPoolExecutor newScaling(int min, int max, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        ExecutorScalingQueue<Runnable> queue = new ExecutorScalingQueue<Runnable>();
        // we force the execution, since we might run into concurrency issues in offer for ScalingBlockingQueue
        EsThreadPoolExecutor executor = new EsThreadPoolExecutor(min, max, keepAliveTime, unit, queue, threadFactory, new ForceQueuePolicy());
        queue.executor = executor;
        return executor;
    }
    //cached
    /*
    核心线程数为0，最大线程数为 Integer.MAX_VALUE   wordQueue使用SynchronousQueue
    handler（因为已达到线程边界和队列容量，新的执行被阻塞时使用的处理程序）  使用EsAbortPolicy

     */
    public static EsThreadPoolExecutor newCached(long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        return new EsThreadPoolExecutor(0, Integer.MAX_VALUE, keepAliveTime, unit, new SynchronousQueue<Runnable>(), threadFactory, new EsAbortPolicy());
    }

    //fixed
    /*
    核心线程数等于最大线程数   wordQueue使用newBlockingQueue或者SizeBlockingQueue

    handler（因为已达到线程边界和队列容量，新的执行被阻塞时使用的处理程序）  使用EsAbortPolicy
     */
    public static EsThreadPoolExecutor newFixed(int size, int queueCapacity, ThreadFactory threadFactory) {
        BlockingQueue<Runnable> queue;
        if (queueCapacity < 0) {
            queue = ConcurrentCollections.newBlockingQueue();
        } else {
            queue = new SizeBlockingQueue<Runnable>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity);
        }
        return new EsThreadPoolExecutor(size, size, 0, TimeUnit.MILLISECONDS, queue, threadFactory, new EsAbortPolicy());
    }

    public static String threadName(Settings settings, String namePrefix) {
        String name = settings.get("name");
        if (name == null) {
            name = "elasticsearch";
        } else {
            name = "elasticsearch[" + name + "]";
        }
        return name + "[" + namePrefix + "]";
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new EsThreadFactory(namePrefix);
    }

    //线程工厂，用于传入到 ThreadPoolExecutor  创建线程时调用newThread
    static class EsThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        public EsThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            // 获取线程组  线程组表示一个线程的集合
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
                    0);
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * Cannot instantiate.
     */
    private EsExecutors() {
    }


    static class ExecutorScalingQueue<E> extends LinkedTransferQueue<E> {

        ThreadPoolExecutor executor;

        public ExecutorScalingQueue() {
        }

        @Override
        public boolean offer(E e) {
            if (!tryTransfer(e)) {
                int left = executor.getMaximumPoolSize() - executor.getCorePoolSize();
                if (left > 0) {
                    return false;
                } else {
                    return super.offer(e);
                }
            } else {
                return true;
            }
        }
    }

    /**
     * A handler for rejected tasks that adds the specified element to this queue,
     * waiting if necessary for space to become available.
     */
    static class ForceQueuePolicy implements XRejectedExecutionHandler {
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                //should never happen since we never wait
                throw new EsRejectedExecutionException(e);
            }
        }

        @Override
        public long rejected() {
            return 0;
        }
    }
}
