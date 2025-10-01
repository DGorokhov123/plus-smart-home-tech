package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
public class ShoppingCartDto {

    @NotBlank
    private String shoppingCartId;

    @NotNull
    @NotEmpty
    private Map<String, Long> products;

}
