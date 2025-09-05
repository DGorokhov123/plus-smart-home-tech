package ru.yandex.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.dto.sensor.*;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

public class GrpcSensorEventMapper {

    public static SpecificRecordBase toAvro(SensorEventProto event) {
        SpecificRecordBase payload = switch (event.getPayloadCase()) {
            case SensorEventProto.PayloadCase.CLIMATE_SENSOR -> toAvro(event.getClimateSensor());
            case SensorEventProto.PayloadCase.LIGHT_SENSOR -> toAvro(event.getLightSensor());
            case SensorEventProto.PayloadCase.MOTION_SENSOR -> toAvro(event.getMotionSensor());
            case SensorEventProto.PayloadCase.SWITCH_SENSOR -> toAvro(event.getSwitchSensor());
            case SensorEventProto.PayloadCase.TEMPERATURE_SENSOR -> toAvro(event.getTemperatureSensor());
            default ->
                    throw new IllegalArgumentException("Unsupported Sensor Event Type: " + event.getPayloadCase());
        };
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(payload)
                .build();
    }

    public static ClimateSensorAvro toAvro(ClimateSensorProto event) {
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setHumidity(event.getHumidity())
                .setCo2Level(event.getCo2Level())
                .build();

    }

    public static LightSensorAvro toAvro(LightSensorProto event) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setLuminosity(event.getLuminosity())
                .build();
    }

    public static MotionSensorAvro toAvro(MotionSensorProto event) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.getMotion())
                .setVoltage(event.getVoltage())
                .build();
    }

    public static SwitchSensorAvro toAvro(SwitchSensorProto event) {
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    public static TemperatureSensorAvro toAvro(TemperatureSensorProto event) {
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
    }

}
