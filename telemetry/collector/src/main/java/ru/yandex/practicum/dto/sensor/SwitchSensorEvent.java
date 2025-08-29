package ru.yandex.practicum.dto.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SwitchSensorEvent extends SensorEvent {

    @NotNull(message = "Field state shouldn't be null")
    private Boolean state;

}
