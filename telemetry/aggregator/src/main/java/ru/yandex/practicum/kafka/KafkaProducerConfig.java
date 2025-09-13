package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.GeneralAvroSerializer;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Configuration
public class KafkaProducerConfig {

    private Producer<Void, SpecificRecordBase> producer;

    @Bean
    public Producer<Void, SpecificRecordBase> getProducer(
            @Value("${smart-home-tech.kafka.bootstrap-servers}") String kafkaBootstrapServers
    ) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        this.producer = new KafkaProducer<>(config);
        return producer;
    }

    @PreDestroy
    public void closeProducer() {
        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofSeconds(10));
            log.info("Kafka Producer is closed");
        }
    }


}
