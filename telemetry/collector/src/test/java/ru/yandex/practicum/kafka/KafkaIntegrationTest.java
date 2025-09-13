package ru.yandex.practicum.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import ru.yandex.practicum.deserializer.HubEventDeserializer;
import ru.yandex.practicum.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.dto.hub.*;
import ru.yandex.practicum.dto.sensor.*;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.service.EventService;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "smart-home-tech.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.VoidSerializer",
                "spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer",
                "spring.kafka.producer.properties.schema.registry.url=mock://test",
                "logging.level.ru.yandex.practicum=DEBUG",
                "grpc.server.inProcessName=test",
                "grpc.server.port=-1",
                "grpc.client.inProcess.address=in-process:test"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"telemetry.sensors.v1", "telemetry.hubs.v1"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EventService eventService;

    @Autowired
    private Producer<Void, SpecificRecordBase> kafkaProducer;

    @LocalServerPort
    private int port;

    @Value("${smart-home-tech.kafka.sensor-event-topic}")
    private String sensorEventTopic;

    @Value("${smart-home-tech.kafka.hub-event-topic}")
    private String hubEventTopic;

    private KafkaConsumer<Void, SpecificRecordBase> sensorConsumer;
    private KafkaConsumer<Void, SpecificRecordBase> hubConsumer;

    @BeforeEach
    void setUp() {
        // Создание потребителя для топика sensor events
        Map<String, Object> sensorConsumerProps = KafkaTestUtils.consumerProps("sensor-group", "true", embeddedKafkaBroker);
        sensorConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        sensorConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        sensorConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        sensorConsumerProps.put("schema.registry.url", "mock://test");
        sensorConsumerProps.put("specific.avro.reader", true);

        sensorConsumer = new KafkaConsumer<>(sensorConsumerProps);
        sensorConsumer.subscribe(Collections.singletonList(sensorEventTopic));

        // Создание потребителя для топика hub events
        Map<String, Object> hubConsumerProps = KafkaTestUtils.consumerProps("hub-group", "true", embeddedKafkaBroker);
        hubConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        hubConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        hubConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class);
        hubConsumerProps.put("schema.registry.url", "mock://test");
        hubConsumerProps.put("specific.avro.reader", true);

        hubConsumer = new KafkaConsumer<>(hubConsumerProps);
        hubConsumer.subscribe(Collections.singletonList(hubEventTopic));
    }

    @AfterEach
    void tearDown() {
        if (sensorConsumer != null) {
            sensorConsumer.close();
        }
        if (hubConsumer != null) {
            hubConsumer.close();
        }
    }

    @Test
    void shouldSendAndReceiveMotionSensorEventViaRest() throws Exception {
        // Given
        MotionSensorEvent motionEvent = new MotionSensorEvent();
        motionEvent.setId("motion-001");
        motionEvent.setHubId("hub-001");
        motionEvent.setTimestamp(Instant.now());
        motionEvent.setLinkQuality(85);
        motionEvent.setMotion(true);
        motionEvent.setVoltage(320);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<MotionSensorEvent> request = new HttpEntity<>(motionEvent, headers);

        // When
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/sensors",
                request,
                Void.class
        );

        // Then
        assertEquals(200, response.getStatusCode().value());

        // Проверяем, что сообщение попало в Kafka
        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        assertEquals(sensorEventTopic, record.topic());

        SpecificRecordBase avroRecord = record.value();
        assertNotNull(avroRecord);
        assertInstanceOf(SensorEventAvro.class, avroRecord);

        SensorEventAvro sensorEventAvro = (SensorEventAvro) avroRecord;
        assertEquals("motion-001", sensorEventAvro.getId());
        assertEquals("hub-001", sensorEventAvro.getHubId());
        assertNotNull(sensorEventAvro.getTimestamp());

        assertInstanceOf(MotionSensorAvro.class, sensorEventAvro.getPayload());
        MotionSensorAvro motionSensorAvro = (MotionSensorAvro) sensorEventAvro.getPayload();
        assertEquals(85, motionSensorAvro.getLinkQuality());
        assertTrue(motionSensorAvro.getMotion());
        assertEquals(320, motionSensorAvro.getVoltage());
    }

    @Test
    void shouldSendAndReceiveTemperatureSensorEventViaRest() throws Exception {
        // Given
        TemperatureSensorEvent tempEvent = new TemperatureSensorEvent();
        tempEvent.setId("temp-001");
        tempEvent.setHubId("hub-001");
        tempEvent.setTimestamp(Instant.now());
        tempEvent.setTemperatureC(22);
        tempEvent.setTemperatureF(72);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<TemperatureSensorEvent> request = new HttpEntity<>(tempEvent, headers);

        // When
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/sensors",
                request,
                Void.class
        );

        // Then
        assertEquals(200, response.getStatusCode().value());

        // Проверяем, что сообщение попало в Kafka
        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        SensorEventAvro sensorEventAvro = (SensorEventAvro) record.value();

        assertEquals("temp-001", sensorEventAvro.getId());
        assertEquals("hub-001", sensorEventAvro.getHubId());

        assertInstanceOf(TemperatureSensorAvro.class, sensorEventAvro.getPayload());
        TemperatureSensorAvro tempSensorAvro = (TemperatureSensorAvro) sensorEventAvro.getPayload();
        assertEquals(22, tempSensorAvro.getTemperatureC());
        assertEquals(72, tempSensorAvro.getTemperatureF());
    }

    @Test
    void shouldSendAndReceiveDeviceAddedHubEventViaRest() throws Exception {
        // Given
        DeviceAddedEvent deviceEvent = new DeviceAddedEvent();
        deviceEvent.setHubId("hub-001");
        deviceEvent.setTimestamp(Instant.now());
        deviceEvent.setId("device-001");
        deviceEvent.setDeviceType(DeviceType.MOTION_SENSOR);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DeviceAddedEvent> request = new HttpEntity<>(deviceEvent, headers);

        // When
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/hubs",
                request,
                Void.class
        );

        // Then
        assertEquals(200, response.getStatusCode().value());

        // Проверяем, что сообщение попало в Kafka
        ConsumerRecords<Void, SpecificRecordBase> records = hubConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        assertEquals(hubEventTopic, record.topic());

        HubEventAvro hubEventAvro = (HubEventAvro) record.value();
        assertEquals("hub-001", hubEventAvro.getHubId());
        assertNotNull(hubEventAvro.getTimestamp());

        assertInstanceOf(DeviceAddedEventAvro.class, hubEventAvro.getPayload());
        DeviceAddedEventAvro deviceAddedAvro = (DeviceAddedEventAvro) hubEventAvro.getPayload();
        assertEquals("device-001", deviceAddedAvro.getId());
        assertEquals(DeviceTypeAvro.MOTION_SENSOR, deviceAddedAvro.getType());
    }

    @Test
    void shouldSendAndReceiveScenarioAddedHubEventViaRest() throws Exception {
        // Given
        ScenarioAddedEvent scenarioEvent = new ScenarioAddedEvent();
        scenarioEvent.setHubId("hub-001");
        scenarioEvent.setTimestamp(Instant.now());
        scenarioEvent.setName("Evening Lights");

        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId("light-001");
        condition.setType(ScenarioConditionType.LUMINOSITY);
        condition.setOperation(ScenarioConditionOperation.LOWER_THAN);
        condition.setValue(50);

        DeviceAction action = new DeviceAction();
        action.setSensorId("switch-001");
        action.setType(DeviceActionType.ACTIVATE);
        action.setValue(null);

        scenarioEvent.setConditions(List.of(condition));
        scenarioEvent.setActions(List.of(action));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<ScenarioAddedEvent> request = new HttpEntity<>(scenarioEvent, headers);

        // When
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/hubs",
                request,
                Void.class
        );

        // Then
        assertEquals(200, response.getStatusCode().value());

        // Проверяем, что сообщение попало в Kafka
        ConsumerRecords<Void, SpecificRecordBase> records = hubConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        HubEventAvro hubEventAvro = (HubEventAvro) record.value();

        assertInstanceOf(ScenarioAddedEventAvro.class, hubEventAvro.getPayload());
        ScenarioAddedEventAvro scenarioAvro = (ScenarioAddedEventAvro) hubEventAvro.getPayload();
        assertEquals("Evening Lights", scenarioAvro.getName());
        assertEquals(1, scenarioAvro.getConditions().size());
        assertEquals(1, scenarioAvro.getActions().size());

        ScenarioConditionAvro conditionAvro = scenarioAvro.getConditions().getFirst();
        assertEquals("light-001", conditionAvro.getSensorId());
        assertEquals(ConditionTypeAvro.LUMINOSITY, conditionAvro.getType());
        assertEquals(ConditionOperationAvro.LOWER_THAN, conditionAvro.getOperation());
        assertEquals(50, conditionAvro.getValue());

        DeviceActionAvro actionAvro = scenarioAvro.getActions().getFirst();
        assertEquals("switch-001", actionAvro.getSensorId());
        assertEquals(ActionTypeAvro.ACTIVATE, actionAvro.getType());
    }

    @Test
    void shouldSendAndReceiveSensorEventViaGrpc() throws InterruptedException {
        // Given
        SensorEventProto sensorEventProto = SensorEventProto.newBuilder()
                .setId("grpc-sensor-001")
                .setHubId("grpc-hub-001")
                .setTimestamp(createTimestamp())
                .setLightSensor(
                        LightSensorProto.newBuilder()
                                .setLinkQuality(90)
                                .setLuminosity(150)
                                .build()
                )
                .build();

        // When
        eventService.handleSensorEvent(sensorEventProto);

        // Даем время на обработку
        Thread.sleep(1000);

        // Then
        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        SensorEventAvro sensorEventAvro = (SensorEventAvro) record.value();

        assertEquals("grpc-sensor-001", sensorEventAvro.getId());
        assertEquals("grpc-hub-001", sensorEventAvro.getHubId());

        assertInstanceOf(LightSensorAvro.class, sensorEventAvro.getPayload());
        LightSensorAvro lightSensorAvro = (LightSensorAvro) sensorEventAvro.getPayload();
        assertEquals(90, lightSensorAvro.getLinkQuality());
        assertEquals(150, lightSensorAvro.getLuminosity());
    }

    @Test
    void shouldSendAndReceiveHubEventViaGrpc() throws InterruptedException {
        // Given
        HubEventProto hubEventProto = HubEventProto.newBuilder()
                .setHubId("grpc-hub-001")
                .setTimestamp(createTimestamp())
                .setDeviceRemoved(
                        DeviceRemovedEventProto.newBuilder()
                                .setId("removed-device-001")
                                .build()
                )
                .build();

        // When
        eventService.handleHubEvent(hubEventProto);

        // Даем время на обработку
        Thread.sleep(1000);

        // Then
        ConsumerRecords<Void, SpecificRecordBase> records = hubConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ConsumerRecord<Void, SpecificRecordBase> record = records.iterator().next();
        HubEventAvro hubEventAvro = (HubEventAvro) record.value();

        assertEquals("grpc-hub-001", hubEventAvro.getHubId());

        assertInstanceOf(DeviceRemovedEventAvro.class, hubEventAvro.getPayload());
        DeviceRemovedEventAvro deviceRemovedAvro = (DeviceRemovedEventAvro) hubEventAvro.getPayload();
        assertEquals("removed-device-001", deviceRemovedAvro.getId());
    }

    @Test
    void shouldHandleMultipleSensorEventsInBatch() throws Exception {
        // Given
        List<SensorEvent> events = new ArrayList<>();

        // Создаем разные типы событий сенсоров
        MotionSensorEvent motion = new MotionSensorEvent();
        motion.setId("batch-motion-001");
        motion.setHubId("batch-hub-001");
        motion.setTimestamp(Instant.now());
        motion.setLinkQuality(80);
        motion.setMotion(true);
        motion.setVoltage(300);
        events.add(motion);

        TemperatureSensorEvent temp = new TemperatureSensorEvent();
        temp.setId("batch-temp-001");
        temp.setHubId("batch-hub-001");
        temp.setTimestamp(Instant.now());
        temp.setTemperatureC(25);
        temp.setTemperatureF(77);
        events.add(temp);

        LightSensorEvent light = new LightSensorEvent();
        light.setId("batch-light-001");
        light.setHubId("batch-hub-001");
        light.setTimestamp(Instant.now());
        light.setLinkQuality(95);
        light.setLuminosity(200);
        events.add(light);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // When - отправляем все события
        for (SensorEvent event : events) {
            HttpEntity<SensorEvent> request = new HttpEntity<>(event, headers);
            ResponseEntity<Void> response = restTemplate.postForEntity(
                    "http://localhost:" + port + "/events/sensors",
                    request,
                    Void.class
            );
            assertEquals(200, response.getStatusCode().value());
        }

        // Then
        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(15));
        assertEquals(3, records.count());

        Set<String> receivedIds = new HashSet<>();
        for (ConsumerRecord<Void, SpecificRecordBase> record : records) {
            SensorEventAvro sensorEventAvro = (SensorEventAvro) record.value();
            receivedIds.add(sensorEventAvro.getId());
        }

        assertTrue(receivedIds.contains("batch-motion-001"));
        assertTrue(receivedIds.contains("batch-temp-001"));
        assertTrue(receivedIds.contains("batch-light-001"));
    }

    @Test
    void shouldPreserveMessageOrderingWithinPartition() throws Exception {
        // Given
        String hubId = "ordering-hub-001";
        List<String> expectedOrder = new ArrayList<>();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // When - отправляем события с одинаковым hubId (попадут в одну партицию)
        for (int i = 0; i < 5; i++) {
            MotionSensorEvent event = new MotionSensorEvent();
            event.setId("order-motion-" + i);
            event.setHubId(hubId);
            event.setTimestamp(Instant.now());
            event.setLinkQuality(80);
            event.setMotion(i % 2 == 0);
            event.setVoltage(300 + i);
            expectedOrder.add("order-motion-" + i);

            HttpEntity<MotionSensorEvent> request = new HttpEntity<>(event, headers);
            ResponseEntity<Void> response = restTemplate.postForEntity(
                    "http://localhost:" + port + "/events/sensors",
                    request,
                    Void.class
            );
            assertEquals(200, response.getStatusCode().value());
        }

        // Then
        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(15));
        assertEquals(5, records.count());

        List<String> actualOrder = new ArrayList<>();
        for (ConsumerRecord<Void, SpecificRecordBase> record : records) {
            SensorEventAvro sensorEventAvro = (SensorEventAvro) record.value();
            actualOrder.add(sensorEventAvro.getId());
        }

        // Проверяем, что порядок сохранился
        assertEquals(expectedOrder, actualOrder);
    }

    @Test
    void shouldHandleClimateAndSwitchSensorEvents() throws Exception {
        // Given - Climate Sensor Event
        ClimateSensorEvent climate = new ClimateSensorEvent();
        climate.setId("climate-001");
        climate.setHubId("test-hub-001");
        climate.setTimestamp(Instant.now());
        climate.setTemperatureC(23);
        climate.setHumidity(45);
        climate.setCo2Level(400);

        // Switch Sensor Event
        SwitchSensorEvent switchEvent = new SwitchSensorEvent();
        switchEvent.setId("switch-001");
        switchEvent.setHubId("test-hub-001");
        switchEvent.setTimestamp(Instant.now());
        switchEvent.setState(true);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // When
        ResponseEntity<Void> climateResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/sensors",
                new HttpEntity<>(climate, headers),
                Void.class
        );

        ResponseEntity<Void> switchResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/events/sensors",
                new HttpEntity<>(switchEvent, headers),
                Void.class
        );

        // Then
        assertEquals(200, climateResponse.getStatusCode().value());
        assertEquals(200, switchResponse.getStatusCode().value());

        ConsumerRecords<Void, SpecificRecordBase> records = sensorConsumer.poll(Duration.ofSeconds(10));
        assertEquals(2, records.count());

        Map<String, SensorEventAvro> eventsById = new HashMap<>();
        for (ConsumerRecord<Void, SpecificRecordBase> record : records) {
            SensorEventAvro event = (SensorEventAvro) record.value();
            eventsById.put(event.getId(), event);
        }

        // Проверяем климатический сенсор
        SensorEventAvro climateEventAvro = eventsById.get("climate-001");
        assertNotNull(climateEventAvro);
        assertInstanceOf(ClimateSensorAvro.class, climateEventAvro.getPayload());
        ClimateSensorAvro climateAvro = (ClimateSensorAvro) climateEventAvro.getPayload();
        assertEquals(23, climateAvro.getTemperatureC());
        assertEquals(45, climateAvro.getHumidity());
        assertEquals(400, climateAvro.getCo2Level());

        // Проверяем переключатель
        SensorEventAvro switchEventAvro = eventsById.get("switch-001");
        assertNotNull(switchEventAvro);
        assertInstanceOf(SwitchSensorAvro.class, switchEventAvro.getPayload());
        SwitchSensorAvro switchAvro = (SwitchSensorAvro) switchEventAvro.getPayload();
        assertTrue(switchAvro.getState());
    }

    // Helper methods

    private Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }
}
