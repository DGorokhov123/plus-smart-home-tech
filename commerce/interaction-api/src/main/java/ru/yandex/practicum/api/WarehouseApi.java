package ru.yandex.practicum.api;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

public interface WarehouseApi {

    // Добавить новый товар на склад.
    @PutMapping("/api/v1/warehouse")
    @ResponseStatus(HttpStatus.OK)
    public String createProduct(
            @RequestBody @Valid NewProductInWarehouseRequest newProductInWarehouseRequest
    );

    // Предварительно проверить что количество товаров на складе достаточно для данной корзины продуктов.
    @PostMapping("/api/v1/warehouse/check")
    @ResponseStatus(HttpStatus.OK)
    public BookedProductsDto checkShoppingCart(
            @RequestBody @Valid ShoppingCartDto shoppingCartDto
    );

    // Принять товар на склад. (изменить кол-во товара)
    @PostMapping("/api/v1/warehouse/add")
    @ResponseStatus(HttpStatus.OK)
    public String changeQuantity(
            @RequestBody @Valid AddProductToWarehouseRequest addProductToWarehouseRequest
    );

    // Предоставить адрес склада для расчёта доставки.
    @GetMapping("/api/v1/warehouse/address")
    @ResponseStatus(HttpStatus.OK)
    public AddressDto getAddress();

}
