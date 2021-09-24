package com.p7rox.esApi.service.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class IndexServiceImpl implements IndexService {

    private final RestHighLevelClient restHighLevelClient;
    private final ObjectMapper objectMapper;

    public String getDocument(String index, String id) {
        return "true";
    }

    public List<Object> getDocuments(String index, Map<String, String> allParams) {

        List<Object> resultList = new ArrayList<>();

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(100);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();
            resultList.addAll(toStringList(hits.getHits()));
            do {
//                System.out.println(hits.getTotalHits().value);
//                System.out.println(hits.getHits().length);
//                System.out.println(scrollId);

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueSeconds(30));
                SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchScrollResponse.getScrollId();
                hits = searchScrollResponse.getHits();
                resultList.addAll(toStringList(hits.getHits()));

            } while (scrollId != null && hits.getHits().length != 0 );

            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClearScrollResponse response = restHighLevelClient.clearScroll(request, RequestOptions.DEFAULT);

            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private List<Object> toStringList(SearchHit[] searchHits) throws Exception {
        List<Object> stringList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            stringList.add(searchHit.getSourceAsMap());
        }
        return stringList;
    }
}
