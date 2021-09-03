package com.p7rox.esApi.service;


import com.p7rox.esApi.entity.Product;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.aggregations.Aggregation;

public interface HighLevelClientProductService {

    Product getProduct(String id);

    Map<String, Long> aggregateTerm(String term);

    Product createProduct(Product product);

    boolean deleteProduct(String id);

    boolean createProductIndex();
}