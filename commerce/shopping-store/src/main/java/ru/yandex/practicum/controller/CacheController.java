package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/cache")
@RequiredArgsConstructor
public class CacheController {

    private final CacheManager cacheManager;

    @GetMapping("/stats/{cacheName}")
    public Map<String, Object> getCacheStats(
            @PathVariable String cacheName
    ) {
        return cacheManager.getCacheNames().stream()
                .map(cacheManager::getCache)
                .filter(c -> c instanceof CaffeineCache)
                .map(c -> (CaffeineCache) c)
                .collect(Collectors.toMap(
                        CaffeineCache::getName,
                        c -> Map.of(
                                "hitCount", c.getNativeCache().stats().hitCount(),
                                "missCount", c.getNativeCache().stats().missCount(),
                                "hitRate", c.getNativeCache().stats().hitRate(),
                                "evictionCount", c.getNativeCache().stats().evictionCount(),
                                "size", c.getNativeCache().estimatedSize()
                        )
                ));
    }

}
