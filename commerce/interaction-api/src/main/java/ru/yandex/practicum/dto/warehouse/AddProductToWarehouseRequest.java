package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

@Data
public class AddProductToWarehouseRequest {

    @NotBlank
    private String productId;

    @NotNull
    @Positive
    private Long quantity;

}
