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
import ru.yandex.practicum.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    private Consumer<Void, SensorEventAvro> sensorConsumer;

    @Bean
    public Consumer<Void, SensorEventAvro> getSensorConsumer(
            @Value("${smart-home-tech.kafka.bootstrap-servers}") String kafkaBootstrapServers,
            @Value("${smart-home-tech.kafka.sensor-event-topic}") String sensorEventTopic
    ) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.sensorConsumer = new KafkaConsumer<>(config);
        sensorConsumer.subscribe(List.of(sensorEventTopic));
        return sensorConsumer;
    }

    @PreDestroy
    public void closeAllConsumers() {
        if (sensorConsumer != null) {
            sensorConsumer.close(Duration.ofSeconds(10));
            log.info("Kafka Sensor Consumer is closed");
        }
    }


}
