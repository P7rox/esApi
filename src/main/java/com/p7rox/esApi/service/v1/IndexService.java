package com.p7rox.esApi.service.v1;

import com.p7rox.esApi.entity.Product;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IndexService {

    Optional<String> getDocument(String index, String id);

    List<String> getDocuments(String index, Map<String,String> allParams);

}
