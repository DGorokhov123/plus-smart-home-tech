package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.api.WarehouseApi;

@FeignClient(name = "warehouse", fallbackFactory = WarehouseClientFallbackFactory.class)
public interface WarehouseClient extends WarehouseApi {

}
