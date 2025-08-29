package ru.yandex.practicum.dto.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ScenarioCondition {

    @NotBlank
    private String sensorId;

    @NotNull
    private ScenarioConditionType type;

    @NotNull
    private ScenarioConditionOperation operation;

    private Integer value;

}
