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


/*
对Recycler   进行包装  最终向外边暴露

继承FilterRecycler  来根据参数  定制  获取到的Recycler.V  对象
 */
abstract class FilterRecycler<T> implements Recycler<T> {

    /** Get the delegate instance to foward calls to. */
    protected abstract Recycler<T> getDelegate();  //获取实例

    /** Wrap a recycled reference. */
    protected Recycler.V<T> wrap(Recycler.V<T> delegate) {
        return delegate;
    }  //包装实例

    @Override
    public Recycler.V<T> obtain(int sizing) {
        return wrap(getDelegate().obtain(sizing));
    }//方法重载

    @Override
    public Recycler.V<T> obtain() {
        return wrap(getDelegate().obtain());
    }  //方法重载

    @Override
    public void close() {
        getDelegate().close();
    }

}
