package com.p7rox.esApi.service.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p7rox.esApi.utils.QueryObject;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class IndexServiceImpl implements IndexService {

    private final RestHighLevelClient restHighLevelClient;
    private final ObjectMapper objectMapper;

    public Object getDocument(String index, String id) throws Exception {
        try {
            SearchResponse response = restHighLevelClient.search(buildRequest(index, id), RequestOptions.DEFAULT);
            return toDocumentList(response.getHits().getHits()).stream().findFirst().orElse(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public List<Object> getDocuments(String index, Map<String, String> allParams) throws Exception {

        List<Object> resultList = new ArrayList<>();
        QueryObject queryObject = new QueryObject(allParams);

        boolean countOnly = queryObject.isCountonly();
        int limit = queryObject.getLimit();

        try {
            SearchResponse searchResponse = restHighLevelClient.search(buildRequestWithOffset(index, queryObject), RequestOptions.DEFAULT);
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
                    SearchResponse searchScrollResponse = restHighLevelClient.search(buildRequestWithSearchAfter(index, queryObject, hits.getAt(hits.getHits().length - 1).getSortValues()), RequestOptions.DEFAULT);
                    hits = searchScrollResponse.getHits();
                    resultList.addAll(toDocumentList(hits.getHits()));
                } while (hits.getHits().length != 0);
            }

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

    private BoolQueryBuilder queryBuilder (QueryObject queryObject) {
        String searchKey = queryObject.getQueryKey();
        String[] SearchValue = queryObject.getQueryValue();
        List<Map<String, String>> filters = queryObject.getFilterMap();

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        if (searchKey == null) query = query.must(QueryBuilders.matchAllQuery());  // Get all records QueryBuilders.matchAllQuery()
        else query = query.must(QueryBuilders.termsQuery(searchKey, SearchValue));

        if (!filters.isEmpty()) {
            for (Map<String, String> filterMap : filters)
                for (Map.Entry<String,String> filter : filterMap.entrySet()) {
//                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), filter.getValue()));
                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), filter.getValue().split(",")));
//                    query = query.filter(QueryBuilders.termsQuery(filter.getKey(), Arrays.asList(filter.getValue().split(","))));
                    System.out.println("filter key: " + filter.getKey() + " and filter value: " + Arrays.asList(filter.getValue().split(",")));
                }
        }
        return query;
    }

    private SearchSourceBuilder sourceBuilder(String id) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(id);
        searchSourceBuilder.query(idsQueryBuilder);
        return searchSourceBuilder;
    }

    private SearchSourceBuilder sourceBuilder(QueryObject queryObject) {
        String[] includeFields = queryObject.getFieldList() == null ? new String[] {"*"} : queryObject.getFieldList();
        String[] excludeFields = new String[] {"_*"};
        boolean countOnly = queryObject.isCountonly();
        int limit = queryObject.getLimit();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(queryBuilder(queryObject));

        if (countOnly) searchSourceBuilder.fetchSource(false); else searchSourceBuilder.fetchSource(includeFields, excludeFields); // CountOnly: Ignoring Source || Selective Fields
        if (limit > 0) searchSourceBuilder.size(limit); else searchSourceBuilder.size(100); //Limit Implementation

        return searchSourceBuilder;
    }

    private SearchSourceBuilder sourceBuilderWithSort (SearchSourceBuilder searchSourceBuilder, QueryObject queryObject) {
        String[] sortList = queryObject.getSortList();
        searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
        return searchSourceBuilder;
    }

    private SearchSourceBuilder sourceBuilderWithOffset (SearchSourceBuilder searchSourceBuilder, QueryObject queryObject) {
        int offset = queryObject.getOffset();
        if (offset > 0) searchSourceBuilder.from(offset);
        return searchSourceBuilder;
    }

    private SearchSourceBuilder sourceBuilderWithSearchAfter (SearchSourceBuilder searchSourceBuilder, Object[] searchAfterSortValues) {
        searchSourceBuilder.searchAfter(searchAfterSortValues);
        return searchSourceBuilder;
    }

    private SearchRequest buildRequest(String index, String id) {
        return new SearchRequest(index).source(sourceBuilder(id));
    }

    private SearchRequest buildRequest(String index, QueryObject queryObject) {
        return new SearchRequest(index).source(sourceBuilder(queryObject));
    }

    private SearchRequest buildRequestWithOffset(String index, QueryObject queryObject) {
        return new SearchRequest(index).source(sourceBuilderWithOffset(sourceBuilderWithSort(sourceBuilder(queryObject), queryObject), queryObject));
    }

    private SearchRequest buildRequestWithSearchAfter(String index, QueryObject queryObject, Object[] searchAfterSortValues) {
        return new SearchRequest(index).source(sourceBuilderWithSearchAfter(sourceBuilderWithSort(sourceBuilder(queryObject), queryObject), searchAfterSortValues));
    }
}
