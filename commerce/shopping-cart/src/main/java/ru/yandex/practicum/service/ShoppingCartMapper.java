package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dal.ShoppingCart;

public class ShoppingCartMapper {

    public static ShoppingCartDto toDto(ShoppingCart cart) {
        ShoppingCartDto dto = new ShoppingCartDto();
        dto.setShoppingCartId(cart.getShoppingCartId());
        dto.setProducts(cart.getProducts());
        return dto;
    }

}
