package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;

@Data
public class ChangeProductQuantityRequest {

    @NotBlank
    private String productId;

    @NotNull
    @PositiveOrZero
    private Long newQuantity;

}
