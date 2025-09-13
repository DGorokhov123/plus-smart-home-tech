package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotPoller implements Runnable {

    private final Consumer<Void, SensorsSnapshotAvro> consumer;
    private final SnapshotService snapshotService;

    @Override
    public void run() {
        log.info("Kafka Snapshot poller started");
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<Void, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                    snapshotService.handleSnapshot(record.value());
                }
                consumer.commitAsync();
            }

        } catch (WakeupException e) {
            // break the cycle
        }
        log.info("Kafka Snapshot poller finished");
    }

}
