package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class BookedProductsDto {

    @NotNull
    private BigDecimal deliveryWeight;

    @NotNull
    private BigDecimal deliveryVolume;

    @NotNull
    private Boolean fragile;

}
