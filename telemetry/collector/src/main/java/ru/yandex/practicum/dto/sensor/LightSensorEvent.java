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
public class LightSensorEvent extends SensorEvent {

    @NotNull @Min(0) @Max(100)
    private Integer linkQuality;

    @NotNull @Min(0) @Max(100)
    private Integer luminosity;

}
