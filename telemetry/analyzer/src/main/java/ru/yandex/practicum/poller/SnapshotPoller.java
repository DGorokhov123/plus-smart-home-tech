package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotPoller implements Runnable {

    private final KafkaProperties kafkaProperties;
    private final SnapshotService snapshotService;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private Consumer<Void, SensorsSnapshotAvro> consumer;

    @Override
    public void run() {
        consumer = getSnapshotConsumer();
        try {
            consumer.subscribe(List.of(kafkaProperties.getSensorSnapshotTopic()));
            log.info("Kafka Snapshot poller started");
            while (true) {
                ConsumerRecords<Void, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                    snapshotService.handleSnapshot(record.value());
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
                    offsets.put(partition, offset);
                }
                if (!records.isEmpty()) consumer.commitAsync(offsets, null);
            }
        } catch (WakeupException e) {
            log.info("Kafka Snapshot Consumer - Caught WakeupException");
        } catch (Exception e) {
            log.error("Unexpected error in Kafka Snapshot poller", e);
        } finally {
            try {
                if (!offsets.isEmpty()) consumer.commitSync(offsets);
            } finally {
                try {
                    log.info("Kafka Snapshot Consumer is going to be closed...");
                    consumer.close(Duration.ofSeconds(10));
                    log.info("Kafka Snapshot Consumer is closed");
                } catch (Exception e) {
                    log.error("Error closing Kafka Snapshot Consumer", e);
                }
            }
        }
        log.info("Kafka Snapshot poller finished");
    }

    private Consumer<Void, SensorsSnapshotAvro> getSnapshotConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getSnapshotGroupId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getAutoOffsetReset());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaProperties.getEnableAutoCommit());
        return new KafkaConsumer<>(config);
    }

    public void closeConsumer() {
        if (consumer == null) return;
        consumer.wakeup();
        log.info("Kafka Snapshot Consumer - WakeupException is thrown!");
    }

}
