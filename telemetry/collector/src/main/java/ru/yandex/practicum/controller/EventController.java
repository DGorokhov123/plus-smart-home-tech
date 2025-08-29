package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.hub.HubEvent;
import ru.yandex.practicum.dto.sensor.SensorEvent;
import ru.yandex.practicum.service.EventService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
@Validated
public class EventController {

    private final EventService eventService;

    @PostMapping("/sensors")
    public ResponseEntity<Void> postSensors(
            @Valid @RequestBody SensorEvent sensorEvent
    ) {
        return eventService.handleSensorEvent(sensorEvent);
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> postHubs(
            @Valid @RequestBody HubEvent hubEvent
    ) {
        return eventService.handleHubEvent(hubEvent);
    }

}
