package disaster.publisher.service.impl;

import disaster.publisher.service.Eservice;
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

@Service
public class EserviceImpl implements Eservice {

    @Autowired
    JestClient jest;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_" + date;
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(builder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult result = null;
        try {
            result = jest.execute(search);
        } catch (IOException e) {
            System.out.println("捕获异常");
            e.printStackTrace();
        }

        Long total = result.getTotal();
        return total;
    }

    @Override
    public Map getDauHour(String date) {
        String indexName = "gmall_dau_info_" + date;
        HashMap<String, Long> resMap = new HashMap<>();
        TermsAggregationBuilder groupbyTH = AggregationBuilders.terms("groupbyTH").field("th.keyword").size(24);
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(groupbyTH);
        Search search = new Search.Builder(builder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult result = null;
        try {
            result = jest.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<TermsAggregation.Entry> list = result.getAggregations().getTermsAggregation("groupbyTH").getBuckets();
        for (TermsAggregation.Entry entry : list) {
            resMap.put(entry.getKey(),entry.getCount());
        }

        return resMap;
    }
}
