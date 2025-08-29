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
public class TemperatureSensorEvent extends SensorEvent {

    @NotNull(message = "Field temperatureC shouldn't be null")
    @Min(value = -273, message = "Field temperatureC is out of bounds")
    @Max(value = 500, message = "Field temperatureC is out of bounds")
    private Integer temperatureC;

    @NotNull(message = "Field temperatureF shouldn't be null")
    @Min(value = -459, message = "Field temperatureF is out of bounds")
    @Max(value = 932, message = "Field temperatureF is out of bounds")
    private Integer temperatureF;

}
