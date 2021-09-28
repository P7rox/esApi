package com.p7rox.esApi.service.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p7rox.esApi.utils.QueryObject;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

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
        int limit = queryObject.getLimit();
        List<Map<String, String>> filters = queryObject.getFilterMap();
        String queryKey = queryObject.getQueryKey();
        String[] queryValue = queryObject.getQueryValue();

        System.out.println(filters.toString());
        System.out.println(queryObject.toString());

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        if (queryKey == null) query = query.must(QueryBuilders.matchAllQuery());
                else query = query.must(QueryBuilders.termsQuery(queryKey, queryValue));

        if (!filters.isEmpty()) {
            for (Map<String, String> filterMap : filters)
                for (Map.Entry<String,String> filter : filterMap.entrySet()) {
//                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), filter.getValue()));
                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), filter.getValue().split(",")));
//                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), Arrays.asList(filter.getValue().split(","))));
                    System.out.println("filter key: " + filter.getKey() + " and filter value: " + Arrays.asList(filter.getValue().split(",")));
                }
        }
//        query = query.filter(QueryBuilders.termsQuery("ticket.inquiryType", "INQTYP01", "INQTYP06"))
//                .filter(QueryBuilders.termsQuery("ticket.status", "NEW", "CLOSED"))
//                .filter(QueryBuilders.termsQuery("ticket.ownership", "OWNED", "OTHER_OWNER"));

        searchSourceBuilder.query(query); // Get all records QueryBuilders.matchAllQuery()

        if (countOnly) searchSourceBuilder.fetchSource(false); else searchSourceBuilder.fetchSource(includeFields, excludeFields); // CountOnly: Ignoring Source || Selective Fields
//        if (offset > 0) searchSourceBuilder.from(offset); // Validation Failed: 1: using [from] is not allowed in a scroll context; ToDo: resolve scroll issue
        if (limit > 0) searchSourceBuilder.size(limit); else searchSourceBuilder.size(100); //Limit Implementation

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();
            Long totalCount = hits.getTotalHits().value < limit ? hits.getTotalHits().value : limit;

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
            return (limit > 100 && limit%100 != 0 ) ? resultList.stream().limit(limit).collect(Collectors.toList()) : resultList;
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
