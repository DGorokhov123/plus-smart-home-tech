package ru.yandex.practicum.service;

import org.springframework.http.ResponseEntity;
import ru.yandex.practicum.dto.hub.HubEvent;
import ru.yandex.practicum.dto.sensor.SensorEvent;

public interface EventService {

    ResponseEntity<Void> handleSensorEvent(SensorEvent sensorEvent);

    ResponseEntity<Void> handleHubEvent(HubEvent hubEvent);

}
