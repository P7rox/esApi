package com.p7rox.esApi.repository;

import com.p7rox.esApi.entity.Product;
import java.util.List;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ProductRepository extends ElasticsearchRepository<Product, String> {

    List<Product> findAllByName(String name);

    @Query("{\"match\":{\"name\":\"?0\"}}")
    List<Product> findAllByNameUsingAnnotations(String name);
}
