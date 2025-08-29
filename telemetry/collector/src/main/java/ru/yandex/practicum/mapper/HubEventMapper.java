package ru.yandex.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.dto.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class HubEventMapper {

    public static SpecificRecordBase toAvro(HubEvent event) {
        SpecificRecordBase payload = switch (event) {
            case DeviceAddedEvent e -> toAvro(e);
            case DeviceRemovedEvent e -> toAvro(e);
            case ScenarioAddedEvent e -> toAvro(e);
            case ScenarioRemovedEvent e -> toAvro(e);
            default -> throw new IllegalArgumentException("Unsupported Hub Event Type: " + event.getClass().getName());
        };
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    public static DeviceAddedEventAvro toAvro(DeviceAddedEvent event) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(toAvro(event.getDeviceType()))
                .build();
    }

    public static DeviceRemovedEventAvro toAvro(DeviceRemovedEvent event) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    public static ScenarioAddedEventAvro toAvro(ScenarioAddedEvent event) {
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(event.getConditions().stream().map(HubEventMapper::toAvro).toList())
                .setActions(event.getActions().stream().map(HubEventMapper::toAvro).toList())
                .build();
    }

    public static ScenarioRemovedEventAvro toAvro(ScenarioRemovedEvent event) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    public static DeviceTypeAvro toAvro(DeviceType type) {
        return switch (type) {
            case DeviceType.CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case DeviceType.LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case DeviceType.MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case DeviceType.SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            case DeviceType.TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            default -> throw new IllegalArgumentException("Unsupported Device Type: " + type);
        };
    }

    public static ScenarioConditionAvro toAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(toAvro(condition.getType()))
                .setOperation(toAvro(condition.getOperation()))
                .setValue(condition.getValue())
                .build();
    }

    public static ConditionTypeAvro toAvro(ScenarioConditionType type) {
        return switch (type) {
            case ScenarioConditionType.MOTION -> ConditionTypeAvro.MOTION;
            case ScenarioConditionType.LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case ScenarioConditionType.SWITCH -> ConditionTypeAvro.SWITCH;
            case ScenarioConditionType.TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case ScenarioConditionType.CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case ScenarioConditionType.HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Type: " + type);
        };
    }

    public static ConditionOperationAvro toAvro(ScenarioConditionOperation operation) {
        return switch (operation) {
            case ScenarioConditionOperation.EQUALS -> ConditionOperationAvro.EQUALS;
            case ScenarioConditionOperation.GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case ScenarioConditionOperation.LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Operation: " + operation);
        };
    }

    public static DeviceActionAvro toAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(toAvro(action.getType()))
                .setValue(action.getValue())
                .build();
    }

    public static ActionTypeAvro toAvro(DeviceActionType type) {
        return switch (type) {
            case DeviceActionType.ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DeviceActionType.DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case DeviceActionType.INVERSE -> ActionTypeAvro.INVERSE;
            case DeviceActionType.SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Type: " + type);
        };
    }

}
