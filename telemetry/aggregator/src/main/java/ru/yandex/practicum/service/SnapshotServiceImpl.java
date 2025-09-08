package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotServiceImpl implements SnapshotService {

    private final Producer<Void, SpecificRecordBase> producer;

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    @Value("${smart-home-tech.kafka.sensor-snapshot-topic}")
    private String sensorSnapshotTopicName;

    @Override
    public void handleSensorEvent(SensorEventAvro event) {
        log.debug("Received: {}", event);
        if (event == null || event.getId() == null || event.getHubId() == null ||
                event.getPayload() == null || event.getTimestamp() == null) {
            log.warn("Event was ignored. Incorrect fields: {}", event);
            return;
        }

        SensorsSnapshotAvro snapshot = snapshots.get(event.getHubId());
        SensorStateAvro state = null;
        if (snapshot != null) state = snapshot.getSensorsState().get(event.getId());

        // Nothing to do if state: 1. Exists 2. Hasn't been changed
        if (state != null && Objects.equals(state.getData(), event.getPayload())) return;

        // Nothing to do if state: 1. Exists 2. Has timestamp later than incoming event
        if (state != null && state.getTimestamp() != null && state.getTimestamp().isAfter(event.getTimestamp())) return;

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        if (snapshot == null) snapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(new HashMap<>())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshots.put(event.getHubId(), snapshot);

        ProducerRecord<Void, SpecificRecordBase> record = new ProducerRecord<>(sensorSnapshotTopicName, snapshot);
        producer.send(record);
        log.debug("Sent: {}", snapshot);
    }

}
