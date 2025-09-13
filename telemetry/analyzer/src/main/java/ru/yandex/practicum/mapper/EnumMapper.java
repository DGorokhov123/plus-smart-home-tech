package ru.yandex.practicum.mapper;

import ru.yandex.practicum.entity.ActionType;
import ru.yandex.practicum.entity.ConditionOperation;
import ru.yandex.practicum.entity.ConditionType;
import ru.yandex.practicum.entity.DeviceType;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

public class EnumMapper {

    // AVRO MAPPING

    public static DeviceType fromAvro(DeviceTypeAvro type) {
        return switch (type) {
            case DeviceTypeAvro.CLIMATE_SENSOR -> DeviceType.CLIMATE_SENSOR;
            case DeviceTypeAvro.LIGHT_SENSOR -> DeviceType.LIGHT_SENSOR;
            case DeviceTypeAvro.MOTION_SENSOR -> DeviceType.MOTION_SENSOR;
            case DeviceTypeAvro.SWITCH_SENSOR -> DeviceType.SWITCH_SENSOR;
            case DeviceTypeAvro.TEMPERATURE_SENSOR -> DeviceType.TEMPERATURE_SENSOR;
            default -> throw new IllegalArgumentException("Unsupported Device Type: " + type);
        };
    }

    public static ConditionType fromAvro(ConditionTypeAvro type) {
        return switch (type) {
            case ConditionTypeAvro.MOTION -> ConditionType.MOTION;
            case ConditionTypeAvro.LUMINOSITY -> ConditionType.LUMINOSITY;
            case ConditionTypeAvro.SWITCH -> ConditionType.SWITCH;
            case ConditionTypeAvro.TEMPERATURE -> ConditionType.TEMPERATURE;
            case ConditionTypeAvro.CO2LEVEL -> ConditionType.CO2LEVEL;
            case ConditionTypeAvro.HUMIDITY -> ConditionType.HUMIDITY;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Type: " + type);
        };
    }

    public static ConditionOperation fromAvro(ConditionOperationAvro operation) {
        return switch (operation) {
            case ConditionOperationAvro.EQUALS -> ConditionOperation.EQUALS;
            case ConditionOperationAvro.GREATER_THAN -> ConditionOperation.GREATER_THAN;
            case ConditionOperationAvro.LOWER_THAN -> ConditionOperation.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unsupported Scenario Condition Operation: " + operation);
        };
    }

    public static ActionType fromAvro(ActionTypeAvro type) {
        return switch (type) {
            case ActionTypeAvro.ACTIVATE -> ActionType.ACTIVATE;
            case ActionTypeAvro.DEACTIVATE -> ActionType.DEACTIVATE;
            case ActionTypeAvro.INVERSE -> ActionType.INVERSE;
            case ActionTypeAvro.SET_VALUE -> ActionType.SET_VALUE;
            default -> throw new IllegalArgumentException("Unsupported Scenario Action Type: " + type);
        };
    }

    // PROTO MAPPING

    public static ActionTypeProto toProto(ActionType type) {
        return switch (type) {
            case ActionType.ACTIVATE -> ActionTypeProto.ACTIVATE;
            case ActionType.DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case ActionType.INVERSE -> ActionTypeProto.INVERSE;
            case ActionType.SET_VALUE -> ActionTypeProto.SET_VALUE;
            default -> throw new IllegalArgumentException("Unsupported Scenario Action Type: " + type);
        };
    }

}
