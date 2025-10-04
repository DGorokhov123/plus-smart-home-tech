package ru.yandex.practicum.grpc;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.*;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled("Выполнять только при запущенных Discovery and Config servers")
@SpringBootTest(
        properties = {
                "grpc.server.inProcessName=test",
                "grpc.server.port=-1",
                "grpc.client.inProcess.address=in-process:test"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class GrpcIntegrationTest {

    @GrpcClient("inProcess")
    private CollectorControllerGrpc.CollectorControllerBlockingStub collectStub;

    @MockitoBean
    private Producer<Void, SpecificRecordBase> kafkaProducer;

    @BeforeEach
    void setUp() {
        Mockito.reset(kafkaProducer);
    }

    @Test
    void shouldCollectMotionSensorEventSuccessfully() {
        // Given
        SensorEventProto sensorEvent = createMotionSensorEvent();

        // When
        Empty response = collectStub.collectSensorEvent(sensorEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));

        ArgumentCaptor<ProducerRecord<Void, SpecificRecordBase>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());

        ProducerRecord<Void, SpecificRecordBase> record = captor.getValue();
        assertEquals("telemetry.sensors.v1", record.topic());
        assertNotNull(record.value());
    }

    @Test
    void shouldCollectTemperatureSensorEventSuccessfully() {
        // Given
        SensorEventProto sensorEvent = createTemperatureSensorEvent();

        // When
        Empty response = collectStub.collectSensorEvent(sensorEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectLightSensorEventSuccessfully() {
        // Given
        SensorEventProto sensorEvent = createLightSensorEvent();

        // When
        Empty response = collectStub.collectSensorEvent(sensorEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectClimateSensorEventSuccessfully() {
        // Given
        SensorEventProto sensorEvent = createClimateSensorEvent();

        // When
        Empty response = collectStub.collectSensorEvent(sensorEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectSwitchSensorEventSuccessfully() {
        // Given
        SensorEventProto sensorEvent = createSwitchSensorEvent();

        // When
        Empty response = collectStub.collectSensorEvent(sensorEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectDeviceAddedHubEventSuccessfully() {
        // Given
        HubEventProto hubEvent = createDeviceAddedHubEvent();

        // When
        Empty response = collectStub.collectHubEvent(hubEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));

        ArgumentCaptor<ProducerRecord<Void, SpecificRecordBase>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());

        ProducerRecord<Void, SpecificRecordBase> record = captor.getValue();
        assertEquals("telemetry.hubs.v1", record.topic());
        assertNotNull(record.value());
    }

    @Test
    void shouldCollectDeviceRemovedHubEventSuccessfully() {
        // Given
        HubEventProto hubEvent = createDeviceRemovedHubEvent();

        // When
        Empty response = collectStub.collectHubEvent(hubEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectScenarioAddedHubEventSuccessfully() {
        // Given
        HubEventProto hubEvent = createScenarioAddedHubEvent();

        // When
        Empty response = collectStub.collectHubEvent(hubEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldCollectScenarioRemovedHubEventSuccessfully() {
        // Given
        HubEventProto hubEvent = createScenarioRemovedHubEvent();

        // When
        Empty response = collectStub.collectHubEvent(hubEvent);

        // Then
        assertNotNull(response);
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldHandleKafkaExceptionInSensorEvent() {
        // Given
        SensorEventProto sensorEvent = createMotionSensorEvent();
        when(kafkaProducer.send(any(ProducerRecord.class)))
                .thenThrow(new RuntimeException("Kafka connection failed"));

        // When & Then
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> collectStub.collectSensorEvent(sensorEvent)
        );

        assertEquals(Status.INTERNAL.getCode(), exception.getStatus().getCode());
        assertTrue(exception.getStatus().getDescription().contains("Kafka connection failed"));
    }

    @Test
    void shouldHandleKafkaExceptionInHubEvent() {
        // Given
        HubEventProto hubEvent = createDeviceAddedHubEvent();
        when(kafkaProducer.send(any(ProducerRecord.class)))
                .thenThrow(new RuntimeException("Kafka connection failed"));

        // When & Then
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> collectStub.collectHubEvent(hubEvent)
        );

        assertEquals(Status.INTERNAL.getCode(), exception.getStatus().getCode());
        assertTrue(exception.getStatus().getDescription().contains("Kafka connection failed"));
    }

    @Test
    void shouldHandleInvalidSensorEventGracefully() {
        // Given - создаем событие без обязательных полей
        SensorEventProto invalidEvent = SensorEventProto.newBuilder().build();

        // When & Then
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> collectStub.collectSensorEvent(invalidEvent)
        );

        assertEquals(Status.INTERNAL.getCode(), exception.getStatus().getCode());
    }

    @Test
    void shouldHandleInvalidHubEventGracefully() {
        // Given - создаем событие без обязательных полей
        HubEventProto invalidEvent = HubEventProto.newBuilder().build();

        // When & Then
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> collectStub.collectHubEvent(invalidEvent)
        );

        assertEquals(Status.INTERNAL.getCode(), exception.getStatus().getCode());
    }

    // Helper methods для создания тестовых данных

    private SensorEventProto createMotionSensorEvent() {
        return SensorEventProto.newBuilder()
                .setId("motion-001")
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setMotionSensor(
                        MotionSensorProto.newBuilder()
                                .setLinkQuality(85)
                                .setMotion(true)
                                .setVoltage(3200)
                                .build()
                )
                .build();
    }

    private SensorEventProto createTemperatureSensorEvent() {
        return SensorEventProto.newBuilder()
                .setId("temp-001")
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setTemperatureSensor(
                        TemperatureSensorProto.newBuilder()
                                .setTemperatureC(22)
                                .setTemperatureF(72)
                                .build()
                )
                .build();
    }

    private SensorEventProto createLightSensorEvent() {
        return SensorEventProto.newBuilder()
                .setId("light-001")
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setLightSensor(
                        LightSensorProto.newBuilder()
                                .setLinkQuality(90)
                                .setLuminosity(150)
                                .build()
                )
                .build();
    }

    private SensorEventProto createClimateSensorEvent() {
        return SensorEventProto.newBuilder()
                .setId("climate-001")
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setClimateSensor(
                        ClimateSensorProto.newBuilder()
                                .setTemperatureC(23)
                                .setHumidity(45)
                                .setCo2Level(400)
                                .build()
                )
                .build();
    }

    private SensorEventProto createSwitchSensorEvent() {
        return SensorEventProto.newBuilder()
                .setId("switch-001")
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setSwitchSensor(
                        SwitchSensorProto.newBuilder()
                                .setState(true)
                                .build()
                )
                .build();
    }

    private HubEventProto createDeviceAddedHubEvent() {
        return HubEventProto.newBuilder()
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setDeviceAdded(
                        DeviceAddedEventProto.newBuilder()
                                .setId("device-001")
                                .setType(DeviceTypeProto.MOTION_SENSOR)
                                .build()
                )
                .build();
    }

    private HubEventProto createDeviceRemovedHubEvent() {
        return HubEventProto.newBuilder()
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setDeviceRemoved(
                        DeviceRemovedEventProto.newBuilder()
                                .setId("device-001")
                                .build()
                )
                .build();
    }

    private HubEventProto createScenarioAddedHubEvent() {
        return HubEventProto.newBuilder()
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setScenarioAdded(
                        ScenarioAddedEventProto.newBuilder()
                                .setName("Evening Lights")
                                .addCondition(
                                        ScenarioConditionProto.newBuilder()
                                                .setSensorId("light-001")
                                                .setType(ConditionTypeProto.LUMINOSITY)
                                                .setOperation(ConditionOperationProto.LOWER_THAN)
                                                .setIntValue(50)
                                                .build()
                                )
                                .addAction(
                                        DeviceActionProto.newBuilder()
                                                .setSensorId("switch-001")
                                                .setType(ActionTypeProto.ACTIVATE)
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    private HubEventProto createScenarioRemovedHubEvent() {
        return HubEventProto.newBuilder()
                .setHubId("hub-001")
                .setTimestamp(createTimestamp())
                .setScenarioRemoved(
                        ScenarioRemovedEventProto.newBuilder()
                                .setName("Evening Lights")
                                .build()
                )
                .build();
    }

    private Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }
}
