package com.atguigu.gmall0105.gmall0105publisher.service.Impl;

import com.atguigu.gmall0105.gmall0105publisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jxy on 2020/12/22 0022 20:46
 */
@Service
public class EsServiceImpl implements EsService {
    @Autowired
    private JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_"+date+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            return searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
    }

    @Override
    public Map getDauHour(String date) {
        Map<String,Long> aggMap = new HashMap<>();
        String indexName = "gmall_dau_info_"+date+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("groupby_hr").field("_2.mi.keyword").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            if(searchResult.getAggregations().getTermsAggregation("groupby_hr")!=null){
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                for(TermsAggregation.Entry bucket:buckets){
                    String key = bucket.getKey();
                    Long count = bucket.getCount();
                    aggMap.put(key,count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return aggMap;
    }
}
