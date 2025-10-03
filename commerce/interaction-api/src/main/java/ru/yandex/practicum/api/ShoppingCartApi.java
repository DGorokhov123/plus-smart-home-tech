package ru.yandex.practicum.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.validation.ValidUsername;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.List;
import java.util.Map;

@RequestMapping("/api/v1/shopping-cart")
public interface ShoppingCartApi {

    // Добавить товар в корзину.
    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto addProductsToCart(
            @RequestParam(required = false) @ValidUsername String username,
            @RequestBody @Valid @NotNull @NotEmpty Map<@NotNull String, @Positive Long> addProductMap
    );

    // Удалить указанные товары из корзины пользователя.
    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto removeProductsFromCart(
            @RequestParam(required = false) @ValidUsername String username,
            @RequestBody @Valid @NotNull @NotEmpty List<@NotNull String> removeProductList
    );

    // Изменить количество товаров в корзине.
    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto changeProductQuantity(
            @RequestParam(required = false) @ValidUsername String username,
            @RequestBody @Valid ChangeProductQuantityRequest changeProductQuantityRequest
    );

    // Деактивация корзины товаров для пользователя.
    @DeleteMapping
    @ResponseStatus(HttpStatus.OK)
    String deactivateCart(
            @RequestParam(required = false) @ValidUsername String username
    );

    // Получить актуальную корзину для авторизованного пользователя.
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto getCartByUsername(
            @RequestParam(required = false) @ValidUsername String username
    );

}
