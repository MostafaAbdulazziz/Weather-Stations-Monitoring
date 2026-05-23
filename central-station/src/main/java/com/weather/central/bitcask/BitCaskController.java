package com.weather.central.bitcask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/bitcask")
public class BitCaskController {

    @Autowired
    private BitCask bitCask;

    @GetMapping("/get")
    public ResponseEntity<Map<String, String>> get(@RequestParam("key") String key) {
        Map<String, String> result = new LinkedHashMap<>();
        try {
            String value = bitCask.get(key);
            result.put("key", key);
            result.put("value", value != null ? value : "NOT FOUND");
            result.put("found", value != null ? "true" : "false");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("error",     e.getClass().getSimpleName());
            result.put("message",   String.valueOf(e.getMessage()));
            result.put("key",       key);
            return ResponseEntity.status(500).body(result);
        }
    }

    @GetMapping("/all")
    public ResponseEntity<Map<String, String>> getAll() {
        Map<String, String> result = new LinkedHashMap<>();
        try {
            List<String> sortedKeys = new ArrayList<>(bitCask.keys());
            Collections.sort(sortedKeys);
            for (String key : sortedKeys) {
                if (key.startsWith("last_sno_")) continue;
                try {
                    String value = bitCask.get(key);
                    result.put(key, value != null ? value : "NULL");
                } catch (Exception e) {
                    result.put(key, "ERROR: " + e.getMessage());
                }
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("error", e.getMessage());
            return ResponseEntity.status(500).body(result);
        }
    }

    @GetMapping("/keys")
    public ResponseEntity<List<String>> keys() {
        try {
            List<String> keys = new ArrayList<>(bitCask.keys());
            Collections.sort(keys);
            keys.removeIf(k -> k.startsWith("last_sno_"));
            return ResponseEntity.ok(keys);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body(List.of("ERROR: " + e.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        try {
            Set<String> allKeys = bitCask.keys();
            List<String> stationKeys = allKeys.stream()
                    .filter(k -> !k.startsWith("last_sno_"))
                    .sorted().toList();
            List<String> internalKeys = allKeys.stream()
                    .filter(k -> k.startsWith("last_sno_"))
                    .sorted().toList();
            status.put("total_keys",    allKeys.size());
            status.put("station_keys",  stationKeys.size());
            status.put("internal_keys", internalKeys.size());
            status.put("stations",      stationKeys);
            status.put("internal",      internalKeys);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            e.printStackTrace();
            status.put("error", e.getMessage());
            return ResponseEntity.status(500).body(status);
        }
    }
}
