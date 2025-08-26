package ru.yandex.practicum.dto.sensor;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ClimateSensorEvent extends SensorEvent {

    @NotNull @Min(-273) @Max(500)
    private Integer temperatureC;

    @NotNull @Min(0) @Max(100)
    private Integer humidity;

    @NotNull @Min(0) @Max(100_000)
    private Integer co2Level;

}
