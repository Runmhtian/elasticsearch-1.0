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

package org.elasticsearch.common.component;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public abstract class AbstractLifecycleComponent<T> extends AbstractComponent implements LifecycleComponent<T> {

    protected final Lifecycle lifecycle = new Lifecycle();  // 组件生命周期

    //组件生命周期监听器   CopyOnWriteArrayList
    /*
    CopyOnWrite容器即写时复制的容器。通俗的理解是当我们往一个容器添加元素的时候，不直接往当前容器添加，而是先将当前容器进行Copy，
    复制出一个新的容器，然后新的容器里添加元素，添加完元素之后，再将原容器的引用指向新的容器。这样做的好处是我们可以对CopyOnWrite
    容器进行并发的读，而不需要加锁，因为当前容器不会添加任何元素。所以CopyOnWrite容器也是一种读写分离的思想，读和写不同的容器。
     */
    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<LifecycleListener>();

    protected AbstractLifecycleComponent(Settings settings) {
        super(settings);
    }

    protected AbstractLifecycleComponent(Settings settings, Class customClass) {
        super(settings, customClass);
    }

    protected AbstractLifecycleComponent(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings) {
        super(settings, prefixSettings);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings, Class customClass) {
        super(settings, prefixSettings, customClass);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings, Class loggerClass, Class componentClass) {
        super(settings, prefixSettings, loggerClass, componentClass);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        listeners.remove(listener);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public T start() throws ElasticsearchException {
        if (!lifecycle.canMoveToStarted()) {
            return (T) this;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeStart();
        }
        doStart();
        lifecycle.moveToStarted();
        for (LifecycleListener listener : listeners) {
            listener.afterStart();
        }
        return (T) this;
    }

    protected abstract void doStart() throws ElasticsearchException;

    @SuppressWarnings({"unchecked"})
    @Override
    public T stop() throws ElasticsearchException {
        if (!lifecycle.canMoveToStopped()) {
            return (T) this;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeStop();
        }
        lifecycle.moveToStopped();
        doStop();
        for (LifecycleListener listener : listeners) {
            listener.afterStop();
        }
        return (T) this;
    }

    protected abstract void doStop() throws ElasticsearchException;

    @Override
    public void close() throws ElasticsearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.canMoveToClosed()) {
            return;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeClose();
        }
        lifecycle.moveToClosed();
        doClose();
        for (LifecycleListener listener : listeners) {
            listener.afterClose();
        }
    }

    protected abstract void doClose() throws ElasticsearchException;
}
