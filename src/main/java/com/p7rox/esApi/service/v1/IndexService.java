package com.p7rox.esApi.service.v1;

import com.p7rox.esApi.entity.Product;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IndexService {

    Object getDocument(String index, String id) throws Exception;

    List<Object> getDocuments(String index, Map<String,String> allParams) throws Exception;

}
