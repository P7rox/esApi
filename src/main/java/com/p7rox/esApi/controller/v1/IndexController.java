package com.p7rox.esApi.controller.v1;

import com.p7rox.esApi.service.v1.IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class IndexController {
    private final IndexService indexService;

    @GetMapping(value="{index}", produces="application/json")
    public List<Object> getProductById(@PathVariable String index, @RequestParam Map<String,String> allParams) throws Exception{
        return indexService.getDocuments(index, allParams);
    }

    @GetMapping(value="{index}/{id}", produces="application/json")
    public Object getProductById(@PathVariable String index, @PathVariable String id) throws Exception {
        return indexService.getDocument(index, id);
    }
}
