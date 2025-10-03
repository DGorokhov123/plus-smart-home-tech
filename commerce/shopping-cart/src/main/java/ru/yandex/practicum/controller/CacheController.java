package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/cache")
@RequiredArgsConstructor
public class CacheController {

    private final CacheManager cacheManager;

    @GetMapping
    public Map<String, Object> getCacheStats() {
        return cacheManager.getCacheNames().stream()
                .map(cacheManager::getCache)
                .filter(c -> c instanceof ConcurrentMapCache)
                .map(c -> (ConcurrentMapCache) c)
                .collect(Collectors.toMap(
                        ConcurrentMapCache::getName,
                        ConcurrentMapCache::getNativeCache
                ));
    }

}
