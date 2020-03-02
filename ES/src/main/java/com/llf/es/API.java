package com.llf.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

public class API {
    public static void main(String[] args) {
//        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("collect_time").from(1000).to(2000);
//        TermsQueryBuilder termsQueryBuilderDevNo = QueryBuilders.termsQuery("dev_no", "dev001");
//        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(rangeQueryBuilder).must(termsQueryBuilderDevNo);
//        TermsAggregationBuilder termsBuilder = AggregationBuilders.terms("aggs_name").
//                field("dev_no").size(10);
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(boolQueryBuilder).aggregation(termsBuilder);
//        String query = searchSourceBuilder.toString();
//        System.out.println(query);

//        ExistsQueryBuilder exist1 = QueryBuilders.existsQuery("collect_time").boost(1.0f);
//        ExistsQueryBuilder exist2 = QueryBuilders.existsQuery("msg_type").boost(1.0f);
//        TermQueryBuilder term = QueryBuilders.termQuery("msg_type","3").boost(1.0f);
//        TermsQueryBuilder terms = QueryBuilders.termsQuery("mac_address",
//                "000000000000","021a11fe1e8c","06696ce732b5").boost(1.0f);
//        RangeQueryBuilder range = QueryBuilders.rangeQuery("collect_time").from("1543078740000").to("1543424340000").includeLower(true).includeUpper(true).boost(1.0f);
//
//        BoolQueryBuilder bool = new BoolQueryBuilder().must(exist1).must(exist2).must(term).must(terms).filter(range);
//
//        CardinalityAggregationBuilder cardinality = AggregationBuilders.cardinality("count").field("mac_address");
//
        RangeQueryBuilder range1 = QueryBuilders.rangeQuery("startTime").gte("0").lte("1579166509000").boost(1.0f);
        RangeQueryBuilder range2 = QueryBuilders.rangeQuery("endTime").gte("0").lte("1579166509000").boost(1.0f);
        ExistsQueryBuilder exist = QueryBuilders.existsQuery("linkFaceBodyGaitId").boost(1.0f);
        String[] values = new String[2];
        values[0] = "1111";
        values[1] = "1111";
        TermsQueryBuilder terms1 = QueryBuilders.termsQuery("linkFaceBodyGaitId", values);
        TermsQueryBuilder terms2 = QueryBuilders.termsQuery("rowKey", values);
        TermsQueryBuilder terms3 = QueryBuilders.termsQuery("cameraIndexCode", values);
        BoolQueryBuilder bool = new BoolQueryBuilder().filter(range1).filter(range2);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(bool).sort("startTime").from(0).size(10);
//        searchSourceBuilder.query(bool).aggregation(cardinality).fetchSource(new String[]{"rowKey"},new String[]{}).sort("collect_time",SortOrder.ASC);
        System.out.println(searchSourceBuilder.toString());



    }
}
