package ru.yandex.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

public class GrpcHubEventMapper {

    public static SpecificRecordBase toAvro(HubEventProto event) {
        SpecificRecordBase payload = switch (event.getPayloadCase()) {
            case HubEventProto.PayloadCase.DEVICE_ADDED -> toAvro(event.getDeviceAdded());
            case HubEventProto.PayloadCase.DEVICE_REMOVED -> toAvro(event.getDeviceRemoved());
            case HubEventProto.PayloadCase.SCENARIO_ADDED -> toAvro(event.getScenarioAdded());
            case HubEventProto.PayloadCase.SCENARIO_REMOVED -> toAvro(event.getScenarioRemoved());
            default -> throw new IllegalArgumentException("Unsupported Hub Event Type: " + event.getPayloadCase());
        };
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(payload)
                .build();
    }

    public static DeviceAddedEventAvro toAvro(DeviceAddedEventProto event) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(toAvro(event.getType()))
                .build();
    }

    public static DeviceRemovedEventAvro toAvro(DeviceRemovedEventProto event) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    public static ScenarioAddedEventAvro toAvro(ScenarioAddedEventProto event) {
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(event.getConditionList().stream().map(GrpcHubEventMapper::toAvro).toList())
                .setActions(event.getActionList().stream().map(GrpcHubEventMapper::toAvro).toList())
                .build();
    }

    public static ScenarioRemovedEventAvro toAvro(ScenarioRemovedEventProto event) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    public static DeviceTypeAvro toAvro(DeviceTypeProto type) {
        return switch (type) {
            case DeviceTypeProto.CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case DeviceTypeProto.LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case DeviceTypeProto.MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case DeviceTypeProto.SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            case DeviceTypeProto.TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            default -> throw new IllegalArgumentException("Unsupported Device Type: " + type);
        };
    }

    public static ScenarioConditionAvro toAvro(ScenarioConditionProto condition) {
        Object value = switch (condition.getValueCase()) {
            case ScenarioConditionProto.ValueCase.BOOL_VALUE -> condition.getBoolValue();
            case ScenarioConditionProto.ValueCase.INT_VALUE -> condition.getIntValue();
            default ->
                    throw new IllegalArgumentException("Unsupported Scenario Condition Value Type: " + condition.getValueCase());
        };
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(toAvro(condition.getType()))
                .setOperation(toAvro(condition.getOperation()))
                .setValue(value)
                .build();
    }

    public static ConditionTypeAvro toAvro(ConditionTypeProto type) {
        return switch (type) {
            case ConditionTypeProto.MOTION -> ConditionTypeAvro.MOTION;
            case ConditionTypeProto.LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case ConditionTypeProto.SWITCH -> ConditionTypeAvro.SWITCH;
            case ConditionTypeProto.TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case ConditionTypeProto.CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case ConditionTypeProto.HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Type: " + type);
        };
    }

    public static ConditionOperationAvro toAvro(ConditionOperationProto operation) {
        return switch (operation) {
            case ConditionOperationProto.EQUALS -> ConditionOperationAvro.EQUALS;
            case ConditionOperationProto.GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case ConditionOperationProto.LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Operation: " + operation);
        };
    }

    public static DeviceActionAvro toAvro(DeviceActionProto action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(toAvro(action.getType()))
                .setValue(action.getValue())
                .build();
    }

    public static ActionTypeAvro toAvro(ActionTypeProto type) {
        return switch (type) {
            case ActionTypeProto.ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case ActionTypeProto.DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case ActionTypeProto.INVERSE -> ActionTypeAvro.INVERSE;
            case ActionTypeProto.SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Type: " + type);
        };
    }

}
