package ru.yandex.practicum.service;

import org.springframework.http.ResponseEntity;
import ru.yandex.practicum.dto.hub.HubEvent;
import ru.yandex.practicum.dto.sensor.SensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface EventService {

    // SENSOR EVENTS

    ResponseEntity<Void> handleSensorEvent(SensorEvent sensorEvent);

    void handleSensorEvent(SensorEventProto sensorEventProto);

    // HUB EVENTS

    ResponseEntity<Void> handleHubEvent(HubEvent hubEvent);

    void handleHubEvent(HubEventProto hubEventProto);

}
