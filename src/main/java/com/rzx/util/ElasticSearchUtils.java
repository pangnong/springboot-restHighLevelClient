package com.rzx.util;

import com.alibaba.fastjson.JSON;
import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.join.aggregations.InternalChildren;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author wuyue
 * @date 2021/12/20 9:31
 */
@Slf4j
public class ElasticSearchUtils {
    @Resource
    private RestHighLevelClient elasticSearchClient;

    /**
     * 创建索引
     *
     * @param index 索引名称
     * @return 创建索引是否成功
     */
    public Boolean createIndex(String index) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        CreateIndexResponse response = elasticSearchClient.indices().create(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 创建索引
     *
     * @param createIndexRequest 创建索引请求，可以自定义映射、配置参数等信息
     * @return 创建索引是否成功
     */
    public Boolean createIndex(CreateIndexRequest createIndexRequest) throws IOException {
        CreateIndexResponse response = elasticSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 更新索引的配置
     *
     * @param settings 索引配置信息
     * @param indices  修改的索引
     * @param <T>      配置类型
     * @return 设置配置是否成功
     */
    public <T> Boolean updateIndicesSettings(Map<String, T> settings, String... indices) throws IOException {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices).settings(settings);
        AcknowledgedResponse response = elasticSearchClient.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 添加索引的映射
     *
     * @param builder 映射内容创建器
     * @param indices 修改的索引
     * @return 修改是否成功
     */
    public Boolean insertIndicesMapping(XContentBuilder builder, String... indices) throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest(indices).source(builder);
        AcknowledgedResponse response = elasticSearchClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 获取指定索引
     *
     * @param indices 需要获取的索引
     * @return 获取所有响应结果
     */
    public GetIndexResponse getIndices(String... indices) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indices);
        GetIndexResponse response = elasticSearchClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * 删除指定的索引
     *
     * @param indices 需要删除的索引
     * @return 删除索引是否成功
     */
    public Boolean deleteIndices(String... indices) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);
        AcknowledgedResponse response = elasticSearchClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 添加指定id的文档到指定索引
     *
     * @param <T>    文档对象类型
     * @param index  索引
     * @param id     文档id
     * @param source 文档对象
     * @return 添加状态
     */
    public <T> RestStatus insertDoc(String index, String id, T source) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index).id(id);
        String sourceJson = JSON.toJSONString(source);
        indexRequest.source(sourceJson, XContentType.JSON);
        IndexResponse response = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
        return response.status();
    }

    /**
     * 修改指定索引的指定id的文档
     *
     * @param <T>   文档对象类型
     * @param index 索引
     * @param id    文档id
     * @param doc   需要添加的文档对象
     * @return 更新结果状态
     */
    public <T> RestStatus updateDoc(String index, String id, T doc) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
        String docJson = JSON.toJSONString(doc);
        updateRequest.doc(docJson, XContentType.JSON);
        UpdateResponse response = elasticSearchClient.update(updateRequest, RequestOptions.DEFAULT);
        return response.status();
    }

    /**
     * 修改指定索引的指定id的文档
     *
     * @param index  索引
     * @param id     文档id
     * @param source 需要修改的键值
     * @return 修改结果状态
     */
    public RestStatus updateDoc(String index, String id, Object... source) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
        updateRequest.doc(source);
        UpdateResponse response = elasticSearchClient.update(updateRequest, RequestOptions.DEFAULT);
        return response.status();
    }

    /**
     * 删除指定索引的指定id的文档
     *
     * @param index 索引
     * @param id    文档id
     * @return 删除结果状态
     */
    public RestStatus deleteDoc(String index, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        DeleteResponse response = elasticSearchClient.delete(deleteRequest, RequestOptions.DEFAULT);
        return response.status();
    }

    /**
     * 根据指定索引的指定id获取文档
     *
     * @param index 索引
     * @param id    文档id
     * @return 获取响应结果
     */
    public GetResponse getDoc(String index, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse response = elasticSearchClient.get(getRequest, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * 搜索文档
     *
     * @param queryBuilder 查询条件创建器
     * @param sortBuilder  排序创建器
     * @param offset       用于分页的起始数据数
     * @param size         分页数据条数
     * @param indices      需要搜索的索引
     * @param <T>          排序器的元素类型
     * @return 搜索响应结果
     */
    public <T extends SortBuilder<T>> SearchResponse searchDoc(@NotNull QueryBuilder queryBuilder, SortBuilder<T> sortBuilder, Integer offset, Integer size, String... indices) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indices);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        if (offset != null && size != null) {
            searchSourceBuilder.from(offset);
            searchSourceBuilder.size(size);
        }
        if (sortBuilder != null) {
            searchSourceBuilder.sort(sortBuilder);
        }
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * 搜索自定义条件的文档
     *
     * @param searchSourceBuilder 搜索条件创建
     * @param indices             索引
     * @return 搜索响应结果
     */
    public SearchResponse searchDoc(SearchSourceBuilder searchSourceBuilder, String... indices) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * @param aggregationBuilders 聚合创建器
     * @param indices             搜索索引
     * @return 聚合搜索响应结果
     */
    public Map<String, Aggregation> aggregationQuery(AggregationBuilder[] aggregationBuilders, String... indices) throws Exception {
        SearchRequest searchRequest = new SearchRequest(indices);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(aggregationBuilders[0]);
        // 多字段聚合
        AggregatorFactories.Builder builder = searchSourceBuilder.aggregations();
        for (int i = 1; i < aggregationBuilders.length; i++) {
            builder.addAggregator(aggregationBuilders[i]);
        }
        searchSourceBuilder.size(0);

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        return response.getAggregations().asMap();
    }

    /**
     * 将聚合搜索结果进行包装
     *
     * @param aggregationMap 聚合结果集合
     * @return 包装后的聚合结果
     */
    public LinkedHashMap<String, Object> warpAgg(Map<String, Aggregation> aggregationMap) {
        LinkedHashMap<String, Object> results = new LinkedHashMap<>();
        for (Map.Entry<String, Aggregation> entry : aggregationMap.entrySet()) {
            String keyword = entry.getKey();
            Aggregation aggregation = entry.getValue();
            LinkedHashMap<String, Object> singleResults = new LinkedHashMap<>();
            // 指标聚合
            if (aggregation instanceof Sum) {
                // sum 求和
                Sum sum = (Sum) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, sum.getValue());
                    return results;
                }
                singleResults.put(sum.getName(), Double.valueOf(sum.getValueAsString()));
            } else if (aggregation instanceof Max) {
                // max 求最大值
                Max max = (Max) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, max.getValue());
                    return results;
                }
                singleResults.put(max.getName(), Double.valueOf(max.getValueAsString()));
            } else if (aggregation instanceof Min) {
                // min 求最小值
                Min min = (Min) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, min.getValue());
                    return results;
                }
                singleResults.put(min.getName(), Double.valueOf(min.getValueAsString()));
            } else if (aggregation instanceof Avg) {
                // avg 求平均值
                Avg avg = (Avg) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, avg.getValue());
                    return results;
                }
                singleResults.put(avg.getName(), Double.valueOf(avg.getValueAsString()));
            } else if (aggregation instanceof ValueCount) {
                // value_count 求数量
                ValueCount valueCount = (ValueCount) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, valueCount.getValue());
                    return results;
                }
                singleResults.put(valueCount.getName(), valueCount.getValue());
            } else if (aggregation instanceof Stats) {
                // stats 统计，求count、max、min、avg、sum五个值
                Stats stats = (Stats) aggregation;
                singleResults.put("count", stats.getCount());
                singleResults.put("min", stats.getMinAsString());
                singleResults.put("max", stats.getMaxAsString());
                singleResults.put("avg", stats.getAvgAsString());
                singleResults.put("sum", stats.getSumAsString());
            } else if (aggregation instanceof Percentiles) {
                // percentiles 占比百分位对应的值统计
                Percentiles percentiles = (Percentiles) aggregation;
                percentiles.forEach(percentile -> singleResults.put(Double.toString(percentile.getPercent()), percentile.getValue()));
            } else if (aggregation instanceof PercentileRanks) {
                // percentile_ranks 统计值小于等于指定值的文档占比
                PercentileRanks percentileRanks = (PercentileRanks) aggregation;
                percentileRanks.forEach(percentileRank -> singleResults.put(Double.toString(percentileRank.getPercent()), percentileRank.getValue()));
            } else if (aggregation instanceof InternalCardinality) {
                // cardinality 去除重复值
                InternalCardinality internalCardinality = (InternalCardinality) aggregation;
                if (aggregationMap.size() == 1) {
                    results.put(keyword, internalCardinality.getValue());
                    return results;
                }
                singleResults.put(internalCardinality.getName(), Double.valueOf(internalCardinality.getValueAsString()));
            }
            // 桶聚合
            else if (aggregation instanceof MultiBucketsAggregation) {
                // terms 分组
                MultiBucketsAggregation terms = (MultiBucketsAggregation) aggregation;
                for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                    if (bucket.getAggregations().asMap().size() == 0) {
                        singleResults.put(bucket.getKeyAsString(), bucket.getDocCount());
                    } else {
                        singleResults.put(bucket.getKeyAsString(), warpAgg(bucket.getAggregations().asMap()));
                    }
                }
            } else if (aggregation instanceof InternalChildren) {
                InternalChildren internalChildren = (InternalChildren) aggregation;
                singleResults.putAll(warpAgg(internalChildren.getAggregations().asMap()));
            } else if (aggregation instanceof InternalFilter) {
                // filter 过滤指定信息
                InternalFilter internalFilter = (InternalFilter) aggregation;
                singleResults.putAll(warpAgg(internalFilter.getAggregations().asMap()));
            } else if (aggregation instanceof InternalNested) {
                InternalNested internalNested = (InternalNested) aggregation;
                singleResults.putAll(warpAgg(internalNested.getAggregations().asMap()));
            } else {
                log.error("unknown class aggregation, agg name : [{}], class : [{}]", aggregation.getName(), aggregation.getClass().getName());
            }
            results.put(keyword, singleResults);
        }
        return results;
    }

}
