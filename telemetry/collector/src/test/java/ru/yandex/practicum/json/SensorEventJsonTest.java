package ru.yandex.practicum.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.yandex.practicum.dto.sensor.ClimateSensorEvent;
import ru.yandex.practicum.dto.sensor.SensorEvent;
import ru.yandex.practicum.dto.sensor.UnknownSensorEvent;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@SpringBootTest
public class SensorEventJsonTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void objectToJsonAndBack() throws JsonProcessingException {
        ClimateSensorEvent climateSensorEvent = new ClimateSensorEvent();
        climateSensorEvent.setId("001");
        climateSensorEvent.setHubId("hub34");
        climateSensorEvent.setTimestamp(Instant.now());
        climateSensorEvent.setTemperatureC(25);
        climateSensorEvent.setHumidity(40);
        climateSensorEvent.setCo2Level(80);
        SensorEvent sensorEvent = climateSensorEvent;
        String json = objectMapper.writeValueAsString(sensorEvent);
        System.out.println(json);

        SensorEvent sensorEvent1 = objectMapper.readValue(json, SensorEvent.class);
        System.out.println(sensorEvent1);

        assertInstanceOf(ClimateSensorEvent.class, sensorEvent1);
    }

    @Test
    void unknownTypeOfEventInJson() throws JsonProcessingException {
        String json = "{\"type\":\"FAKE_SENSOR_EVENT\",\"id\":\"001\",\"hubId\":\"hub34\"" +
                ",\"timestamp\":\"2025-08-25T19:27:12.554570600Z\",\"temperatureC\":25,\"humidity\":40,\"co2Level\":80}";
        SensorEvent sensorEvent = objectMapper.readValue(json, SensorEvent.class);
        System.out.println(sensorEvent);
        assertInstanceOf(UnknownSensorEvent.class, sensorEvent);
    }

}
