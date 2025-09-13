package ru.yandex.practicum.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import ru.yandex.practicum.GeneralAvroSerializer;
import ru.yandex.practicum.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.poller.SensorEventPoller;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "${smart-home-tech.kafka.sensor-event-topic}",
                "${smart-home-tech.kafka.sensor-snapshot-topic}"
        },
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
@TestPropertySource(properties = {
        "smart-home-tech.kafka.bootstrap-servers=localhost:9092",
        "smart-home-tech.kafka.sensor-event-topic=test.sensors.v1",
        "smart-home-tech.kafka.sensor-snapshot-topic=test.snapshots.v1",
        "logging.level.ru.yandex.practicum=DEBUG"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KafkaIntegrationTest {

    private static final String SENSOR_EVENT_TOPIC = "test.sensors.v1";
    private static final String SENSOR_SNAPSHOT_TOPIC = "test.snapshots.v1";
    private static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(10);
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private SensorEventPoller sensorEventPoller;
    private Producer<Void, SensorEventAvro> testProducer;
    private Consumer<Void, SensorsSnapshotAvro> testConsumer;

    @BeforeEach
    void setUp() {
        // Настройка тестового продюсера для отправки SensorEventAvro
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        testProducer = new KafkaProducer<>(producerProps);

        // Настройка тестового консьюмера для чтения SensorsSnapshotAvro
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(Collections.singletonList(SENSOR_SNAPSHOT_TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (testProducer != null) {
            testProducer.close(Duration.ofSeconds(5));
        }
        if (testConsumer != null) {
            testConsumer.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Timeout(30)
    void testSingleSensorEventProcessing() throws Exception {
        // Arrange
        String hubId = "hub-001";
        String sensorId = "sensor-001";
        Instant timestamp = Instant.now();

        TemperatureSensorAvro temperatureData = TemperatureSensorAvro.newBuilder()
                .setTemperatureC(22)
                .setTemperatureF(72)
                .build();

        SensorEventAvro sensorEvent = SensorEventAvro.newBuilder()
                .setId(sensorId)
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setPayload(temperatureData)
                .build();

        // Act - запуск поллера в отдельном потоке
        CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
            sensorEventPoller.startPolling();
        });

        // Даем время на запуск поллера
        Thread.sleep(2000);

        // Отправка события
        ProducerRecord<Void, SensorEventAvro> record = new ProducerRecord<>(SENSOR_EVENT_TOPIC, sensorEvent);
        testProducer.send(record).get(5, TimeUnit.SECONDS);
        testProducer.flush();

        // Assert - чтение результата из snapshot топика
        ConsumerRecords<Void, SensorsSnapshotAvro> records = testConsumer.poll(CONSUMER_POLL_TIMEOUT);
        assertThat(records).isNotEmpty();

        ConsumerRecord<Void, SensorsSnapshotAvro> snapshotRecord = records.iterator().next();
        SensorsSnapshotAvro snapshot = snapshotRecord.value();

        assertNotNull(snapshot);
        assertEquals(hubId, snapshot.getHubId());
        assertTrue(Duration.between(timestamp, snapshot.getTimestamp()).getSeconds() < 3);
        assertNotNull(snapshot.getSensorsState());
        assertTrue(snapshot.getSensorsState().containsKey(sensorId));

        SensorStateAvro state = snapshot.getSensorsState().get(sensorId);
        assertTrue(Duration.between(timestamp, state.getTimestamp()).getSeconds() < 3);
        assertEquals(temperatureData, state.getData());
    }

    @Test
    @Timeout(30)
    void testMultipleSensorEventsFromSameHub() throws Exception {
        // Arrange
        String hubId = "hub-002";
        String sensor1Id = "sensor-001";
        String sensor2Id = "sensor-002";
        Instant timestamp1 = Instant.now();
        Instant timestamp2 = timestamp1.plusSeconds(1);

        MotionSensorAvro motionData = MotionSensorAvro.newBuilder()
                .setLinkQuality(100)
                .setMotion(true)
                .setVoltage(3000)
                .build();

        LightSensorAvro lightData = LightSensorAvro.newBuilder()
                .setLinkQuality(95)
                .setLuminosity(500)
                .build();

        SensorEventAvro event1 = SensorEventAvro.newBuilder()
                .setId(sensor1Id)
                .setHubId(hubId)
                .setTimestamp(timestamp1)
                .setPayload(motionData)
                .build();

        SensorEventAvro event2 = SensorEventAvro.newBuilder()
                .setId(sensor2Id)
                .setHubId(hubId)
                .setTimestamp(timestamp2)
                .setPayload(lightData)
                .build();

        // Act - запуск поллера
        CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
            sensorEventPoller.startPolling();
        });

        Thread.sleep(2000);

        // Отправка событий
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, event1)).get(5, TimeUnit.SECONDS);
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, event2)).get(5, TimeUnit.SECONDS);
        testProducer.flush();

        // Assert - должно быть минимум 2 snapshot записи
        int snapshotCount = 0;
        SensorsSnapshotAvro lastSnapshot = null;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            ConsumerRecords<Void, SensorsSnapshotAvro> records = testConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                snapshotCount++;
                lastSnapshot = record.value();
            }
            if (snapshotCount >= 2) break;
        }

        assertTrue(snapshotCount >= 2, "Expected at least 2 snapshots");
        assertNotNull(lastSnapshot);
        assertEquals(hubId, lastSnapshot.getHubId());
        assertEquals(2, lastSnapshot.getSensorsState().size());
        assertTrue(lastSnapshot.getSensorsState().containsKey(sensor1Id));
        assertTrue(lastSnapshot.getSensorsState().containsKey(sensor2Id));
    }

    @Test
    @Timeout(30)
    void testDuplicateEventIgnored() throws Exception {
        // Arrange
        String hubId = "hub-003";
        String sensorId = "sensor-001";
        Instant timestamp = Instant.now();

        SwitchSensorAvro switchData = SwitchSensorAvro.newBuilder()
                .setState(true)
                .build();

        SensorEventAvro sensorEvent = SensorEventAvro.newBuilder()
                .setId(sensorId)
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setPayload(switchData)
                .build();

        // Act - запуск поллера
        CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
            sensorEventPoller.startPolling();
        });

        Thread.sleep(2000);

        // Отправка одного и того же события дважды
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, sensorEvent)).get(5, TimeUnit.SECONDS);
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, sensorEvent)).get(5, TimeUnit.SECONDS);
        testProducer.flush();

        // Assert - должен быть только один snapshot
        int snapshotCount = 0;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 5000) {
            ConsumerRecords<Void, SensorsSnapshotAvro> records = testConsumer.poll(Duration.ofSeconds(1));
            snapshotCount += records.count();
        }

        assertEquals(1, snapshotCount, "Expected only 1 snapshot for duplicate events");
    }

    @Test
    @Timeout(30)
    void testOlderEventIgnored() throws Exception {
        // Arrange
        String hubId = "hub-004";
        String sensorId = "sensor-001";
        Instant newerTimestamp = Instant.now();
        Instant olderTimestamp = newerTimestamp.minusSeconds(10);

        ClimateSensorAvro climateData1 = ClimateSensorAvro.newBuilder()
                .setTemperatureC(20)
                .setHumidity(50)
                .setCo2Level(400)
                .build();

        ClimateSensorAvro climateData2 = ClimateSensorAvro.newBuilder()
                .setTemperatureC(25)
                .setHumidity(60)
                .setCo2Level(500)
                .build();

        SensorEventAvro newerEvent = SensorEventAvro.newBuilder()
                .setId(sensorId)
                .setHubId(hubId)
                .setTimestamp(newerTimestamp)
                .setPayload(climateData1)
                .build();

        SensorEventAvro olderEvent = SensorEventAvro.newBuilder()
                .setId(sensorId)
                .setHubId(hubId)
                .setTimestamp(olderTimestamp)
                .setPayload(climateData2)
                .build();

        // Act - запуск поллера
        CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
            sensorEventPoller.startPolling();
        });

        Thread.sleep(2000);

        // Отправка сначала нового события, затем старого
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, newerEvent)).get(5, TimeUnit.SECONDS);
        Thread.sleep(1000);
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, olderEvent)).get(5, TimeUnit.SECONDS);
        testProducer.flush();

        // Assert - проверяем, что в последнем snapshot остались данные от newer event
        Thread.sleep(2000);

        SensorsSnapshotAvro lastSnapshot = null;
        ConsumerRecords<Void, SensorsSnapshotAvro> records;
        while ((records = testConsumer.poll(Duration.ofSeconds(1))).count() > 0) {
            for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                lastSnapshot = record.value();
            }
        }

        assertNotNull(lastSnapshot);
        SensorStateAvro state = lastSnapshot.getSensorsState().get(sensorId);
        assertTrue(Duration.between(newerTimestamp, state.getTimestamp()).getSeconds() < 2);
        assertEquals(climateData1, state.getData());
    }

    @Test
    @Timeout(30)
    void testMultipleHubsProcessing() throws Exception {
        // Arrange
        String hub1Id = "hub-101";
        String hub2Id = "hub-102";
        String sensor1Id = "sensor-001";
        String sensor2Id = "sensor-002";
        Instant timestamp = Instant.now();

        SensorEventAvro event1 = SensorEventAvro.newBuilder()
                .setId(sensor1Id)
                .setHubId(hub1Id)
                .setTimestamp(timestamp)
                .setPayload(TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(20)
                        .setTemperatureF(68)
                        .build())
                .build();

        SensorEventAvro event2 = SensorEventAvro.newBuilder()
                .setId(sensor2Id)
                .setHubId(hub2Id)
                .setTimestamp(timestamp.plusSeconds(1))
                .setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(90)
                        .setLuminosity(300)
                        .build())
                .build();

        // Act - запуск поллера
        CompletableFuture<Void> pollingFuture = CompletableFuture.runAsync(() -> {
            sensorEventPoller.startPolling();
        });

        Thread.sleep(2000);

        // Отправка событий от разных хабов
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, event1)).get(5, TimeUnit.SECONDS);
        testProducer.send(new ProducerRecord<>(SENSOR_EVENT_TOPIC, event2)).get(5, TimeUnit.SECONDS);
        testProducer.flush();

        // Assert - должно быть 2 разных snapshot для разных хабов
        Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000 && snapshots.size() < 2) {
            ConsumerRecords<Void, SensorsSnapshotAvro> records = testConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                snapshots.put(record.value().getHubId(), record.value());
            }
        }

        assertEquals(2, snapshots.size(), "Expected snapshots for 2 different hubs");
        assertTrue(snapshots.containsKey(hub1Id));
        assertTrue(snapshots.containsKey(hub2Id));

        SensorsSnapshotAvro hub1Snapshot = snapshots.get(hub1Id);
        assertEquals(1, hub1Snapshot.getSensorsState().size());
        assertTrue(hub1Snapshot.getSensorsState().containsKey(sensor1Id));

        SensorsSnapshotAvro hub2Snapshot = snapshots.get(hub2Id);
        assertEquals(1, hub2Snapshot.getSensorsState().size());
        assertTrue(hub2Snapshot.getSensorsState().containsKey(sensor2Id));
    }
}