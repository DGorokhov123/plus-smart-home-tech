package ru.yandex.practicum.dto.sensor;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ClimateSensorEvent extends SensorEvent {

    @NotNull(message = "Field temperatureC shouldn't be null")
    @Min(value = -273, message = "Field temperatureC is out of bounds")
    @Max(value = 500, message = "Field temperatureC is out of bounds")
    private Integer temperatureC;

    @NotNull(message = "Field humidity shouldn't be null")
    @Min(value = 0, message = "Field humidity is out of bounds")
    @Max(value = 100, message = "Field humidity is out of bounds")
    private Integer humidity;

    @NotNull(message = "Field co2Level shouldn't be null")
    @Min(value = 0, message = "Field co2Level is out of bounds")
    @Max(value = 100_000, message = "Field co2Level is out of bounds")
    private Integer co2Level;

}
