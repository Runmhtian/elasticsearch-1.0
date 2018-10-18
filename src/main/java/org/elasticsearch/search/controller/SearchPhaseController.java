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

package org.elasticsearch.search.controller;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.Suggest;

import java.util.*;

/**
 *
 */
public class SearchPhaseController extends AbstractComponent {

    public static Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>> QUERY_RESULT_ORDERING = new Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>>() {
        @Override
        public int compare(AtomicArray.Entry<? extends QuerySearchResultProvider> o1, AtomicArray.Entry<? extends QuerySearchResultProvider> o2) {
            int i = o1.value.shardTarget().index().compareTo(o2.value.shardTarget().index());
            if (i == 0) {
                i = o1.value.shardTarget().shardId() - o2.value.shardTarget().shardId();
            }
            return i;
        }
    };

    public static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final CacheRecycler cacheRecycler;
    private final boolean optimizeSingleShard;

    @Inject
    public SearchPhaseController(Settings settings, CacheRecycler cacheRecycler) {
        super(settings);
        this.cacheRecycler = cacheRecycler;
        this.optimizeSingleShard = componentSettings.getAsBoolean("optimize_single_shard", true);
    }

    public boolean optimizeSingleShard() {
        return optimizeSingleShard;
    }

    public AggregatedDfs aggregateDfs(AtomicArray<DfsSearchResult> results) {
        ObjectObjectOpenHashMap<Term, TermStatistics> termStatistics = HppcMaps.newNoNullKeysMap();
        ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
        long aggMaxDoc = 0;
        for (AtomicArray.Entry<DfsSearchResult> lEntry : results.asList()) {
            final Term[] terms = lEntry.value.terms();
            final TermStatistics[] stats = lEntry.value.termStatistics();
            assert terms.length == stats.length;
            for (int i = 0; i < terms.length; i++) {
                assert terms[i] != null;
                TermStatistics existing = termStatistics.get(terms[i]);
                if (existing != null) {
                    assert terms[i].bytes().equals(existing.term());
                    // totalTermFrequency is an optional statistic we need to check if either one or both
                    // are set to -1 which means not present and then set it globally to -1
                    termStatistics.put(terms[i], new TermStatistics(existing.term(),
                            existing.docFreq() + stats[i].docFreq(),
                            optionalSum(existing.totalTermFreq(), stats[i].totalTermFreq())));
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }
            final boolean[] states = lEntry.value.fieldStatistics().allocated;
            final Object[] keys = lEntry.value.fieldStatistics().keys;
            final Object[] values = lEntry.value.fieldStatistics().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    String key = (String) keys[i];
                    CollectionStatistics value = (CollectionStatistics) values[i];
                    assert key != null;
                    CollectionStatistics existing = fieldStatistics.get(key);
                    if (existing != null) {
                        CollectionStatistics merged = new CollectionStatistics(
                                key, existing.maxDoc() + value.maxDoc(),
                                optionalSum(existing.docCount(), value.docCount()),
                                optionalSum(existing.sumTotalTermFreq(), value.sumTotalTermFreq()),
                                optionalSum(existing.sumDocFreq(), value.sumDocFreq())
                        );
                        fieldStatistics.put(key, merged);
                    } else {
                        fieldStatistics.put(key, value);
                    }
                }
            }
            aggMaxDoc += lEntry.value.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }

    private static long optionalSum(long left, long right) {
        return Math.min(left, right) == -1 ? -1 : left + right;
    }

    public ScoreDoc[] sortDocs(AtomicArray<? extends QuerySearchResultProvider> resultsArr) {
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results = resultsArr.asList();
        if (results.isEmpty()) {
            return EMPTY_DOCS;
        }

        if (optimizeSingleShard) {  //默认为true  单分片优化   返回结果只有一个分片的结果有数据  则排序可以优化  直接就是有序的
            boolean canOptimize = false;
            QuerySearchResult result = null;  //记录有result的分片result
            int shardIndex = -1;
            if (results.size() == 1) {  //只有一个分片
                canOptimize = true;
                result = results.get(0).value.queryResult();
                shardIndex = results.get(0).index;
            } else {  //不是只有一个分片结果，但是只有一个分片结果 含有doc   也就是每个分片的查询结果ScoreDoc[]大于0
                // lets see if we only got hits from a single shard, if so, we can optimize...
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : results) {
                    if (entry.value.queryResult().topDocs().scoreDocs.length > 0) {
                        if (result != null) { // we already have one, can't really optimize
                            canOptimize = false;
                            break;
                        }
                        canOptimize = true;
                        result = entry.value.queryResult();
                        shardIndex = entry.index;
                    }
                }
            }
            if (canOptimize) {  //直接单分片返回结果
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                if (scoreDocs.length < result.from()) {  //查询的from
                    return EMPTY_DOCS;
                }
                int resultDocsSize = result.size();
                if ((scoreDocs.length - result.from()) < resultDocsSize) {
                    resultDocsSize = scoreDocs.length - result.from();
                }
                if (result.topDocs() instanceof TopFieldDocs) {
                    ScoreDoc[] docs = new ScoreDoc[resultDocsSize];
                    for (int i = 0; i < resultDocsSize; i++) {
                        ScoreDoc scoreDoc = scoreDocs[result.from() + i];
                        scoreDoc.shardIndex = shardIndex;
                        docs[i] = scoreDoc;
                    }
                    return docs;
                } else {
                    ScoreDoc[] docs = new ScoreDoc[resultDocsSize];
                    for (int i = 0; i < resultDocsSize; i++) {
                        ScoreDoc scoreDoc = scoreDocs[result.from() + i];
                        scoreDoc.shardIndex = shardIndex;
                        docs[i] = scoreDoc;
                    }
                    return docs;
                }
            }
        }

        @SuppressWarnings("unchecked")
        AtomicArray.Entry<? extends QuerySearchResultProvider>[] sortedResults = results.toArray(new AtomicArray.Entry[results.size()]);
        Arrays.sort(sortedResults, QUERY_RESULT_ORDERING);  //先按照索引排序
        QuerySearchResultProvider firstResult = sortedResults[0].value;

        int totalNumDocs = 0;

        int queueSize = firstResult.queryResult().from() + firstResult.queryResult().size();  //from+size
        if (firstResult.includeFetch()) {  //说明是  query and fetch  或者是dfs query and fetch
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            // this is also important since we shortcut and fetch only docs from "from" and up to "size"
            //若是query and fetch的话 queue大小应该为   (from+size)*查询分片的个数
            queueSize *= sortedResults.length;
        }

        // we don't use TopDocs#merge here because with TopDocs#merge, when pagination, we need to ask for "from + size" topN
        // hits, which ends up creating a "from + size" ScoreDoc[], while in our implementation, we can actually get away with
        // just create "size" ScoreDoc (the reverse order in the queue). would be nice to improve TopDocs#merge to allow for
        // it in which case we won't need this logic...

        PriorityQueue queue;
        if (firstResult.queryResult().topDocs() instanceof TopFieldDocs) {   //结果 按照field 进行排序  若是查询指定了field排序，则lucene返回的结果得分也是按照
            //字段顺序进行的，因此es这边处理结果的时候也要按照字段进行排序，实际上得分就是排序  若是指定了排序字段  则默认打分机制（匹配度 tfidf）就会无效
            // 默认打分  也就是相关度排序，  sort 指定排序，  boost改变文档或者文档某个字段的权重
            // sorting, first if the type is a String, chance CUSTOM to STRING so we handle nulls properly (since our CUSTOM String sorting might return null)
            TopFieldDocs fieldDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            for (int i = 0; i < fieldDocs.fields.length; i++) {
                boolean allValuesAreNull = true;
                boolean resolvedField = false;
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : sortedResults) {
                    for (ScoreDoc doc : entry.value.queryResult().topDocs().scoreDocs) {
                        FieldDoc fDoc = (FieldDoc) doc;
                        if (fDoc.fields[i] != null) {
                            allValuesAreNull = false;
                            if (fDoc.fields[i] instanceof String) {   //转换字段类型
                                fieldDocs.fields[i] = new SortField(fieldDocs.fields[i].getField(), SortField.Type.STRING, fieldDocs.fields[i].getReverse());
                            }
                            resolvedField = true;
                            break;
                        }
                    }
                    if (resolvedField) {
                        break;
                    }
                }
                if (!resolvedField && allValuesAreNull && fieldDocs.fields[i].getField() != null) {
                    // we did not manage to resolve a field (and its not score or doc, which have no field), and all the fields are null (which can only happen for STRING), make it a STRING
                    fieldDocs.fields[i] = new SortField(fieldDocs.fields[i].getField(), SortField.Type.STRING, fieldDocs.fields[i].getReverse());
                }
            }
            queue = new ShardFieldDocSortedHitQueue(fieldDocs.fields, queueSize);  //优先级队列 fields就是优先级队列的排序逻辑

            // we need to accumulate for all and then filter the from
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : sortedResults) {  //遍历result
                QuerySearchResult result = entry.value.queryResult();
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                totalNumDocs += scoreDocs.length;
                for (ScoreDoc doc : scoreDocs) {
                    doc.shardIndex = entry.index;
                    if (queue.insertWithOverflow(doc) == doc) {   //添加到队列中 若是添加不进去  则break内循环  内循环后边的都添加不进去
                        // filled the queue, break
                        break;
                    }
                }
            }
        } else {  //使用 ScoreDocQueue优先级队列  应该是使用score进行排序
            queue = new ScoreDocQueue(queueSize); // we need to accumulate for all and then filter the from
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : sortedResults) {
                QuerySearchResult result = entry.value.queryResult();
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                totalNumDocs += scoreDocs.length;
                for (ScoreDoc doc : scoreDocs) {
                    doc.shardIndex = entry.index;
                    if (queue.insertWithOverflow(doc) == doc) {
                        // filled the queue, break
                        break;
                    }
                }
            }

        }

        int resultDocsSize = firstResult.queryResult().size();  //结果
        if (firstResult.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            resultDocsSize *= sortedResults.length; //size * 查询分片数
        }
        if (totalNumDocs < queueSize) {  //结果队列没有满   也就是queue中有totalNumDocs个元素
            resultDocsSize = totalNumDocs - firstResult.queryResult().from();
        }

        if (resultDocsSize <= 0) {
            return EMPTY_DOCS;
        }

        // we only pop the first, this handles "from" nicely since the "from" are down the queue
        // that we already fetched, so we are actually popping the "from" and up to "size"
        ScoreDoc[] shardDocs = new ScoreDoc[resultDocsSize];
        for (int i = resultDocsSize - 1; i >= 0; i--)      // put docs in array
            shardDocs[i] = (ScoreDoc) queue.pop();
        return shardDocs;  //返回排序过的 doc
    }

    /**
     * Builds an array, with potential null elements, with docs to load.
     */
    public void fillDocIdsToLoad(AtomicArray<IntArrayList> docsIdsToLoad, ScoreDoc[] shardDocs) {
        for (ScoreDoc shardDoc : shardDocs) {
            IntArrayList list = docsIdsToLoad.get(shardDoc.shardIndex);
            if (list == null) {
                list = new IntArrayList(); // can't be shared!, uses unsafe on it later on
                docsIdsToLoad.set(shardDoc.shardIndex, list);
            }
            list.add(shardDoc.doc);
        }
    }

    //合并得到查询结果
    public InternalSearchResponse merge(ScoreDoc[] sortedDocs, AtomicArray<? extends QuerySearchResultProvider> queryResultsArr, AtomicArray<? extends FetchSearchResultProvider> fetchResultsArr) {

        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults = queryResultsArr.asList();
        List<? extends AtomicArray.Entry<? extends FetchSearchResultProvider>> fetchResults = fetchResultsArr.asList();

        if (queryResults.isEmpty()) {
            return InternalSearchResponse.empty();
        }

        QuerySearchResult firstResult = queryResults.get(0).value.queryResult();

        boolean sorted = false;
        int sortScoreIndex = -1;
        if (firstResult.topDocs() instanceof TopFieldDocs) {
            sorted = true;
            TopFieldDocs fieldDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            for (int i = 0; i < fieldDocs.fields.length; i++) {
                if (fieldDocs.fields[i].getType() == SortField.Type.SCORE) {
                    sortScoreIndex = i;
                }
            }
        }

        // merge facets
        InternalFacets facets = null;
        if (!queryResults.isEmpty()) {
            // we rely on the fact that the order of facets is the same on all query results
            if (firstResult.facets() != null && firstResult.facets().facets() != null && !firstResult.facets().facets().isEmpty()) {
                List<Facet> aggregatedFacets = Lists.newArrayList();
                List<Facet> namedFacets = Lists.newArrayList();
                for (Facet facet : firstResult.facets()) {
                    // aggregate each facet name into a single list, and aggregate it
                    namedFacets.clear();
                    for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                        for (Facet facet1 : entry.value.queryResult().facets()) {
                            if (facet.getName().equals(facet1.getName())) {
                                namedFacets.add(facet1);
                            }
                        }
                    }
                    if (!namedFacets.isEmpty()) {
                        Facet aggregatedFacet = ((InternalFacet) namedFacets.get(0)).reduce(new InternalFacet.ReduceContext(cacheRecycler, namedFacets));
                        aggregatedFacets.add(aggregatedFacet);
                    }
                }
                facets = new InternalFacets(aggregatedFacets);
            }
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        long totalHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut = false;
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
            QuerySearchResult result = entry.value.queryResult();
            if (result.searchTimedOut()) {
                timedOut = true;
            }
            totalHits += result.topDocs().totalHits;
            if (!Float.isNaN(result.topDocs().getMaxScore())) {
                maxScore = Math.max(maxScore, result.topDocs().getMaxScore());
            }
        }
        if (Float.isInfinite(maxScore)) {
            maxScore = Float.NaN;
        }

        // clean the fetch counter
        for (AtomicArray.Entry<? extends FetchSearchResultProvider> entry : fetchResults) {
            entry.value.fetchResult().initCounter();
        }

        // merge hits
        List<InternalSearchHit> hits = new ArrayList<InternalSearchHit>();
        if (!fetchResults.isEmpty()) {
            for (ScoreDoc shardDoc : sortedDocs) {
                FetchSearchResultProvider fetchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                if (fetchResultProvider == null) {
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                int index = fetchResult.counterGetAndIncrement();
                if (index < fetchResult.hits().internalHits().length) {
                    InternalSearchHit searchHit = fetchResult.hits().internalHits()[index];
                    searchHit.score(shardDoc.score);
                    searchHit.shard(fetchResult.shardTarget());

                    if (sorted) {
                        FieldDoc fieldDoc = (FieldDoc) shardDoc;
                        searchHit.sortValues(fieldDoc.fields);
                        if (sortScoreIndex != -1) {
                            searchHit.score(((Number) fieldDoc.fields[sortScoreIndex]).floatValue());
                        }
                    }

                    hits.add(searchHit);
                }
            }
        }

        // merge suggest results
        Suggest suggest = null;
        if (!queryResults.isEmpty()) {
            final Map<String, List<Suggest.Suggestion>> groupedSuggestions = new HashMap<String, List<Suggest.Suggestion>>();
            boolean hasSuggestions = false;
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                Suggest shardResult = entry.value.queryResult().queryResult().suggest();

                if (shardResult == null) {
                    continue;
                }
                hasSuggestions = true;
                Suggest.group(groupedSuggestions, shardResult);
            }

            suggest = hasSuggestions ? new Suggest(Suggest.Fields.SUGGEST, Suggest.reduce(groupedSuggestions)) : null;
        }

        // merge addAggregation
        InternalAggregations aggregations = null;
        if (!queryResults.isEmpty()) {
            if (firstResult.aggregations() != null && firstResult.aggregations().asList() != null) {
                List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(queryResults.size());
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                    aggregationsList.add((InternalAggregations) entry.value.queryResult().aggregations());
                }
                aggregations = InternalAggregations.reduce(aggregationsList, cacheRecycler);
            }
        }

        InternalSearchHits searchHits = new InternalSearchHits(hits.toArray(new InternalSearchHit[hits.size()]), totalHits, maxScore);

        return new InternalSearchResponse(searchHits, facets, aggregations, suggest, timedOut);
    }

}
