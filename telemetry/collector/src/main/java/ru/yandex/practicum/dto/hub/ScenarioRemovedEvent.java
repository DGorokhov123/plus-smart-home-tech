package ru.yandex.practicum.dto.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ScenarioRemovedEvent extends HubEvent {

    @NotBlank
    @Size(min = 3, max = 2147483647)
    private String name;

}
