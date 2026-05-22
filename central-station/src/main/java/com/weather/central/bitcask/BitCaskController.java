package com.weather.central.bitcask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/bitcask")
public class BitCaskController {

    @Autowired
    private BitCask bitCask;

    // GET /bitcask/get?key=1
    @GetMapping("/get")
    public Map<String, String> get(@RequestParam String key) throws IOException {
        String value = bitCask.get(key);
        Map<String, String> result = new HashMap<>();
        result.put("key",   key);
        result.put("value", value != null ? value : "NOT FOUND");
        return result;
    }

    // GET /bitcask/all
    @GetMapping("/all")
    public Map<String, String> getAll() throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : bitCask.keys()) {
            result.put(key, bitCask.get(key));
        }
        return result;
    }

    // GET /bitcask/keys
    @GetMapping("/keys")
    public Set<String> keys() {
        return bitCask.keys();
    }
}
