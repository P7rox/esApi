package com.p7rox.esApi.service.v1;

import com.p7rox.esApi.entity.Product;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class IndexServiceImpl implements IndexService{

    private final RestHighLevelClient restHighLevelClient;

    public Optional<String> getDocument(String index, String id) {
        return "true";
    }

    public List<String> getDocuments(String index, Map<String,String> allParams) {

        
        List<String> documentList = new ArrayList<>();
        return documentList;
    }
}
