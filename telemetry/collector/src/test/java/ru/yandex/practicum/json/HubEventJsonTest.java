package ru.yandex.practicum.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.yandex.practicum.dto.hub.DeviceAddedEvent;
import ru.yandex.practicum.dto.hub.DeviceType;
import ru.yandex.practicum.dto.hub.HubEvent;
import ru.yandex.practicum.dto.hub.UnknownHubEvent;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@Disabled("Выполнять только при запущенных Discovery and Config servers")
@SpringBootTest
public class HubEventJsonTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void objectToJsonAndBack() throws JsonProcessingException {
        DeviceAddedEvent deviceAddedEvent = new DeviceAddedEvent();
        deviceAddedEvent.setHubId("hub34");
        deviceAddedEvent.setTimestamp(Instant.now());
        deviceAddedEvent.setId("001");
        deviceAddedEvent.setDeviceType(DeviceType.MOTION_SENSOR);
        HubEvent hubEvent = deviceAddedEvent;
        String json = objectMapper.writeValueAsString(hubEvent);
        System.out.println(json);

        HubEvent hubEvent1 = objectMapper.readValue(json, HubEvent.class);
        System.out.println(hubEvent1);

        assertInstanceOf(DeviceAddedEvent.class, hubEvent1);
    }

    @Test
    void unknownTypeOfEventInJson() throws JsonProcessingException {
        String json = "{\"type\":\"DEVICE_FAKE\",\"hubId\":\"hub34\",\"timestamp\":\"2025-08-27T15:41:05.904532300Z\",\"id\":\"001\",\"deviceType\":\"MOTION_SENSOR\"}";
        HubEvent hubEvent = objectMapper.readValue(json, HubEvent.class);
        System.out.println(hubEvent);
        assertInstanceOf(UnknownHubEvent.class, hubEvent);
    }

}
