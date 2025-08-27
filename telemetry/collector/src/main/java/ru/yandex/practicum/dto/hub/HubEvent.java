package ru.yandex.practicum.dto.hub;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.time.Instant;

@Data
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = UnknownHubEvent.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED"),
})
public abstract class HubEvent {

    @NotBlank
    private String hubId;

    private Instant timestamp = Instant.now();

}
