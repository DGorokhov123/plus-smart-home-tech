package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class SensorEventPoller {

    private final Consumer<Void, SensorEventAvro> sensorConsumer;
    private final SnapshotService snapshotService;

    public void startPolling() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(sensorConsumer::wakeup));

            while (true) {
                ConsumerRecords<Void, SensorEventAvro> records = sensorConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Void, SensorEventAvro> record : records) {
                    snapshotService.handleSensorEvent(record.value());
                }
                sensorConsumer.commitAsync();
            }

        } catch (WakeupException e) {
            // break the cycle
        }
    }

}
