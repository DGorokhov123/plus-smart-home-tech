package ru.yandex.practicum.dto.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DeviceAction {

    @NotBlank
    private String sensorId;

    @NotNull
    private DeviceActionType type;

    private Integer value;

}
