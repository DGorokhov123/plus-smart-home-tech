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
public class MotionSensorEvent extends SensorEvent {

    @NotNull(message = "Field linkQuality shouldn't be null")
    @Min(value = 0, message = "Field linkQuality is out of bounds")
    @Max(value = 100, message = "Field linkQuality is out of bounds")
    private Integer linkQuality;

    @NotNull(message = "Field motion shouldn't be null")
    private Boolean motion;

    @NotNull(message = "Field voltage shouldn't be null")
    @Min(value = 0, message = "Field voltage is out of bounds")
    @Max(value = 500, message = "Field voltage is out of bounds")
    private Integer voltage;

}
