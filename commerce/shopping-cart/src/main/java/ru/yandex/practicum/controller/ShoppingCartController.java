package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.api.ShoppingCartApi;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.service.ShoppingCartService;

import java.util.List;
import java.util.Map;

@Validated
@RestController
@RequiredArgsConstructor
public class ShoppingCartController implements ShoppingCartApi {

    private final ShoppingCartService shoppingCartService;

    // Добавить товар в корзину.
    @Override
    public ShoppingCartDto addProductsToCart(String username, Map<String, Long> addProductMap) {
        return shoppingCartService.addProductsToCart(username, addProductMap);
    }

    // Удалить указанные товары из корзины пользователя.
    @Override
    public ShoppingCartDto removeProductsFromCart(String username, List<String> removeProductList) {
        return shoppingCartService.removeProductsFromCart(username, removeProductList);
    }

    // Изменить количество товаров в корзине.
    @Override
    public ShoppingCartDto changeProductQuantity(
            String username,
            ChangeProductQuantityRequest changeProductQuantityRequest
    ) {
        return shoppingCartService.changeProductQuantity(username, changeProductQuantityRequest);
    }

    // Деактивация корзины товаров для пользователя.
    @Override
    public String deactivateCart(String username) {
        return shoppingCartService.deactivateCart(username);
    }

    // Получить актуальную корзину для авторизованного пользователя.
    @Override
    public ShoppingCartDto getCartByUsername(String username) {
        return shoppingCartService.getCartByUsername(username);
    }

}
