package ru.yandex.practicum.exception.warehouse;

public class ProductInShoppingCartLowQuantityInWarehouseException extends RuntimeException {

    public ProductInShoppingCartLowQuantityInWarehouseException(String message) {
        super(message);
    }

}
