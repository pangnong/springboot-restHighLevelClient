package com.rzx;

import com.alibaba.fastjson.JSON;
import com.rzx.pojo.User;
import com.rzx.util.ElasticSearchUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author wuyue
 * @date 2021/12/22 10:36
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ApplicationTest {
    @Resource
    private ElasticSearchUtils elasticSearchUtils;

    /**
     * 测试创建索引方法，传入String类型的索引名称
     */
    @Test
    public void testCreateIndex() throws IOException {
        Boolean status = elasticSearchUtils.createIndex("test_02");
        System.out.println("status: " + status);
    }

    /**
     * 测试创建索引方法，传入自定义的CreateIndexRequest
     */
    @Test
    public void testTestCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test_02");
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 2);
        createIndexRequest.settings(settings);
        Boolean status = elasticSearchUtils.createIndex(createIndexRequest);
        System.out.println("status: " + status);
    }

    /**
     * 测试更新索引配置方法，传入配置信息和索引信息
     */
    @Test
    public void testUpdateIndicesSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_replicas", 1);
        String[] indices = new String[]{"test_02"};
        Boolean status = elasticSearchUtils.updateIndicesSettings(settings, indices);
        System.out.println("status: " + status);
    }

    /**
     * 测试添加映射配置方法，传入配置信息和索引信息
     */
    @Test
    public void testUpdateIndicesMapping() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("phone").field("type", "text").endObject()
                .startObject("love").field("type", "text").field("index", "false").endObject()
                .endObject()
                .endObject();
        String[] indices = new String[]{"test_02"};
        Boolean status = elasticSearchUtils.insertIndicesMapping(xContentBuilder, indices);
        System.out.println("status: " + status);
    }

    /**
     * 测试获取索引方法
     */
    @Test
    public void testGetIndices() throws IOException {
        GetIndexResponse indices = elasticSearchUtils.getIndices("user", "test_01", "test_02");
        Arrays.stream(indices.getIndices()).forEach(System.out::println);
    }

    /**
     * 测试删除索引方法
     */
    @Test
    public void testDeleteIndices() throws IOException {
        Boolean status = elasticSearchUtils.deleteIndices("test_02");
        System.out.println("status: " + status);
    }

    /**
     * 测试添加文档方法
     */
    @Test
    public void testInsertDoc() throws IOException {
        RestStatus restStatus = elasticSearchUtils.insertDoc("test_01", "1008", new User("tom", "female", 18));
        System.out.println("restStatus: " + restStatus.name());
    }

    /**
     * 测试修改文档方法
     */
    @Test
    public void testUpdateDoc() throws IOException {
        RestStatus restStatus = elasticSearchUtils.updateDoc("test_01", "1008", new User("cat", "male", null));
        System.out.println("restStatus: " + restStatus.name());
    }

    /**
     * 测试修改文档方法
     */
    @Test
    public void testTestUpdateDoc() throws IOException {
        RestStatus restStatus = elasticSearchUtils.updateDoc("test_01", "1008", "name", "tom", "age", 20);
        System.out.println("restStatus: " + restStatus.name());
    }

    /**
     * 测试删除文档方法
     */
    @Test
    public void testDeleteDoc() throws IOException {
        RestStatus restStatus = elasticSearchUtils.deleteDoc("test_01", "1008");
        System.out.println("restStatus: " + restStatus.name());
    }

    /**
     * 测试查询文档方法
     */
    @Test
    public void testGetDoc() throws IOException {
        GetResponse response = elasticSearchUtils.getDoc("test_01", "1008");
        System.out.println(response.getSourceAsString());
    }

    /**
     * 测试搜索文档方法
     */
    @Test
    public void testSearchDoc() throws IOException {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("age").order(SortOrder.DESC);
        SearchResponse response = elasticSearchUtils.searchDoc(matchAllQueryBuilder, sortBuilder, null, null, "test_01");
        Arrays.stream(response.getHits().getHits()).forEach(System.out::println);
    }

    /**
     * 测试搜索文档方法
     */
    @Test
    public void testTestSearchDoc() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name", "age"}, new String[]{});
        searchSourceBuilder.sort("age", SortOrder.DESC);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(4);
        SearchResponse response = elasticSearchUtils.searchDoc(searchSourceBuilder, "test_01");
        Arrays.stream(response.getHits().getHits()).forEach(System.out::println);
    }

    @Test
    public void testAggregationQuery() throws Exception {
        // 创建多字段、嵌套聚合创建器
        AggregationBuilder[] aggregationBuilders = new AggregationBuilder[]{
                AggregationBuilders.terms("sex_group").subAggregation(
                        AggregationBuilders.terms("age_group").subAggregation(
                                AggregationBuilders.sum("age_sum").field("age")
                        ).field("age")
                ).field("sex.keyword"),
                AggregationBuilders.terms("age_group").field("age")
        };
        // 准备搜索字段
        String[] indices = new String[]{"test_01"};
        // 搜索结果
        Map<String, Aggregation> stringAggregationMap = elasticSearchUtils.aggregationQuery(aggregationBuilders, indices);

        // 将Agg的结果进行包装
        LinkedHashMap<String, Object> warpAgg = elasticSearchUtils.warpAgg(stringAggregationMap);
        System.out.println(formatJsonString(JSON.toJSONString(warpAgg)));
    }

    /**
     * 打印格式化Json字符串
     */
    private String formatJsonString(String s) {
        StringBuilder jsonForMatStr = new StringBuilder();
        int level = 0;
        // 将字符串中的字符逐个按行输出
        for (int index = 0; index < s.length(); index++) {
            // 获取s中的每个字符
            char c = s.charAt(index);
            // level大于0并且jsonForMatStr中的最后一个字符为\n,jsonForMatStr加入\t
            if (level > 0 && '\n' == jsonForMatStr.charAt(jsonForMatStr.length() - 1)) {
                jsonForMatStr.append(getLevelStr(level));
            }
            // 遇到"{"和"["要增加空格和换行，遇到"}"和"]"要减少空格，以对应，遇到","要换行
            switch (c) {
                case '{':
                case '[':
                    jsonForMatStr.append(c).append("\n");
                    level++;
                    break;
                case ',':
                    jsonForMatStr.append(c).append("\n");
                    break;
                case '}':
                case ']':
                    jsonForMatStr.append("\n");
                    level--;
                    jsonForMatStr.append(getLevelStr(level));
                    jsonForMatStr.append(c);
                    break;
                default:
                    jsonForMatStr.append(c);
                    break;
            }
        }
        return jsonForMatStr.toString();
    }

    /**
     * 打印制表符
     */
    private String getLevelStr(int level) {
        StringBuilder levelStr = new StringBuilder();
        for (int levelI = 0; levelI < level; levelI++) {
            levelStr.append("\t");
        }
        return levelStr.toString();
    }
}
