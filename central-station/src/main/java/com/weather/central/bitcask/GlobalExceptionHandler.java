package com.weather.central.bitcask;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleAll(Exception e) {
        e.printStackTrace(); // always print full stack to console
        Map<String, String> body = new LinkedHashMap<>();
        body.put("exception", e.getClass().getName());
        body.put("message",   e.getMessage() != null ? e.getMessage() : "null");

        // Walk the cause chain and include it
        Throwable cause = e.getCause();
        int depth = 0;
        while (cause != null && depth < 5) {
            body.put("cause_" + depth,
                     cause.getClass().getName() + ": " + cause.getMessage());
            cause = cause.getCause();
            depth++;
        }
        return ResponseEntity.internalServerError().body(body);
    }
}
