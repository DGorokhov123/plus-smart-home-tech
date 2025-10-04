package ru.yandex.practicum.client;

import feign.FeignException;
import feign.RetryableException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class WarehouseClientFallbackFactory implements FallbackFactory<WarehouseClient> {

    @Override
    public WarehouseClient create(Throwable cause) {
        return new WarehouseClient() {

            @Override
            public String createProduct(NewProductInWarehouseRequest newProductInWarehouseRequest) {
                throw new UnsupportedOperationException("Method createProduct() is not supported");
            }

            @Override
            public BookedProductsDto checkShoppingCart(ShoppingCartDto shoppingCartDto) {
                if (cause instanceof FeignException.UnprocessableEntity e) {
                    throw e;
                } else if (cause instanceof CallNotPermittedException e) {
                    log.warn("[{}] Circuit Breaker is OPEN for Warehouse service", cause.getClass().getSimpleName());
                } else if (cause instanceof FeignException.FeignServerException e) {
                    log.warn("[{}] Server Error 5xx-code on Warehouse service", cause.getClass().getSimpleName());
                } else if (cause instanceof FeignException.FeignClientException e) {
                    log.warn("[{}] Client Error 4xx-code on Warehouse service", cause.getClass().getSimpleName());
                } else if (cause instanceof RetryableException e) {
                    log.warn("[{}] Timeout calling Warehouse service", cause.getClass().getSimpleName());
                } else if (cause instanceof TimeoutException e) {
                    log.warn("[{}] Concurrent Timeout calling Warehouse service", cause.getClass().getSimpleName());
                } else if (cause instanceof IOException e) {
                    log.warn("[{}] Network Error calling Warehouse service", cause.getClass().getSimpleName());
                } else {
                    log.warn("[{}] Unknown Error calling Warehouse service", cause.getClass().getSimpleName());
                }
                return null;
            }

            @Override
            public String changeQuantity(AddProductToWarehouseRequest addProductToWarehouseRequest) {
                throw new UnsupportedOperationException("Method changeQuantity() is not supported");
            }

            @Override
            public AddressDto getAddress() {
                throw new UnsupportedOperationException("Method getAddress() is not supported");
            }

        };
    }

}
