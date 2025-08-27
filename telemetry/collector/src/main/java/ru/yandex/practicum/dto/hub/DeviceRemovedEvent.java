package ru.yandex.practicum.dto.hub;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DeviceRemovedEvent extends HubEvent {

    @NotBlank
    private String id;

}
