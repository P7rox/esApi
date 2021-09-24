package com.p7rox.esApi.controller.v1;

import com.p7rox.esApi.service.product.SpringDataProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class IndexController {
    private final SpringDataProductService springDataProductService;

    @GetMapping("{index}")
    public String getProductById(@PathVariable String index, @RequestParam Map<String,String> allParams) {
        return "Index is "+ index + " and Parameters are " + allParams.entrySet();
    }

    @GetMapping("{index}/{id}")
    public String getProductById(@PathVariable String index, @PathVariable String id) {
        return "Index is "+ index + " and ids is " + id;
    }
}
