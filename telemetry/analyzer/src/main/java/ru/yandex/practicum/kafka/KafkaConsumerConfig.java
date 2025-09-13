package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.deserializer.HubEventDeserializer;
import ru.yandex.practicum.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    private Consumer<Void, SensorsSnapshotAvro> snapshotConsumer;
    private Consumer<Void, HubEventAvro> hubEventConsumer;

    @Bean
    public Consumer<Void, SensorsSnapshotAvro> getSnapshotConsumer(
            @Value("${smart-home-tech.kafka.bootstrap-servers}") String kafkaBootstrapServers,
            @Value("${smart-home-tech.kafka.sensor-snapshot-topic}") String topic
    ) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.snapshotConsumer = new KafkaConsumer<>(config);
        snapshotConsumer.subscribe(List.of(topic));
        return snapshotConsumer;
    }

    @Bean
    public Consumer<Void, HubEventAvro> getHubEventConsumer(
            @Value("${smart-home-tech.kafka.bootstrap-servers}") String kafkaBootstrapServers,
            @Value("${smart-home-tech.kafka.hub-event-topic}") String topic
    ) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.hubEventConsumer = new KafkaConsumer<>(config);
        hubEventConsumer.subscribe(List.of(topic));
        return hubEventConsumer;
    }

    @PreDestroy
    public void closeAllConsumers() {
        if (snapshotConsumer != null) {
            snapshotConsumer.close(Duration.ofSeconds(10));
            log.info("Kafka Snapshot Consumer is closed");
        }
        if (hubEventConsumer != null) {
            hubEventConsumer.close(Duration.ofSeconds(10));
            log.info("Kafka Hub Event Consumer is closed");
        }
    }

}
