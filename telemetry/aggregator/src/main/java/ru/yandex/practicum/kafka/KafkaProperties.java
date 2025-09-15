package ru.yandex.practicum.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties("smart-home-tech.kafka")
public class KafkaProperties {

    private String bootstrapServers;
    private String hubEventTopic;
    private String sensorEventTopic;
    private String sensorSnapshotTopic;
    private String groupId;
    private String autoOffsetReset;
    private String enableAutoCommit;

}
