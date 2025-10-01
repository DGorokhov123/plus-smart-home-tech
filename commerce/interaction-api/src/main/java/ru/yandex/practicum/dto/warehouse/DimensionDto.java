package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class DimensionDto {

    @NotNull
    private BigDecimal width;

    @NotNull
    private BigDecimal height;

    @NotNull
    private BigDecimal depth;

}
