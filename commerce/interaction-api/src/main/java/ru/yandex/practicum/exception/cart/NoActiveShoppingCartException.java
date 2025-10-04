package ru.yandex.practicum.exception.cart;

public class NoActiveShoppingCartException extends RuntimeException {

    public NoActiveShoppingCartException(String message) {
        super(message);
    }

}
