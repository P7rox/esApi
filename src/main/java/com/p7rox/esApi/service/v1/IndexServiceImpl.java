package com.p7rox.esApi.service.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p7rox.esApi.utils.QueryObject;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class IndexServiceImpl implements IndexService {

    private final RestHighLevelClient restHighLevelClient;
    private final ObjectMapper objectMapper;

    public Object getDocument(String index, String id) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(id);
        sourceBuilder.query(idsQueryBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            return toDocumentList(response.getHits().getHits()).stream().findFirst().orElse(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public List<Object> getDocuments(String index, Map<String, String> allParams) throws Exception {

        List<Object> resultList = new ArrayList<>();
        QueryObject queryObject = new QueryObject(allParams);

        String[] includeFields = queryObject.getFieldList() == null ? new String[] {"*"} : queryObject.getFieldList();
        String[] excludeFields = new String[] {"_*"};

        String searchKey = queryObject.getQueryKey();
        String[] SearchValue = queryObject.getQueryValue();
        boolean countOnly = queryObject.isCountonly();
        int offset = queryObject.getOffset();

        System.out.println(queryObject.toString());

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        if (countOnly) searchSourceBuilder.fetchSource(false); else searchSourceBuilder.fetchSource(includeFields, excludeFields);
        if (offset > 0) searchSourceBuilder.from(offset);
        searchSourceBuilder.size(100);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();
            Long totalCount = hits.getTotalHits().value;

            if (countOnly) {
                Map<String,Long> map = new HashMap<String,Long>() {{put("Record Count", totalCount);}};
                resultList.add(map);
                return resultList;
            }

            resultList.addAll(toDocumentList(hits.getHits()));
            if (resultList.size() < hits.getTotalHits().value) {
                do {
//                System.out.println(hits.getTotalHits().value);
//                System.out.println(hits.getHits().length);
//                System.out.println(scrollId);

                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(TimeValue.timeValueSeconds(30));
                    SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                    scrollId = searchScrollResponse.getScrollId();
                    hits = searchScrollResponse.getHits();
                    resultList.addAll(toDocumentList(hits.getHits()));

                } while (scrollId != null && hits.getHits().length != 0);
            }
            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(request, RequestOptions.DEFAULT);
            boolean succeeded = clearScrollResponse.isSucceeded();
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private List<Object> toDocumentList(SearchHit[] searchHits) throws Exception {
        List<Object> stringList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            stringList.add(searchHit.getSourceAsMap());
        }
        return stringList;
    }
}
