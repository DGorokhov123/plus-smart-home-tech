package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class NewProductInWarehouseRequest {

    @NotBlank
    private String productId;

    private Boolean fragile;

    @NotNull
    private DimensionDto dimension;

    @NotNull
    private BigDecimal weight;

}
