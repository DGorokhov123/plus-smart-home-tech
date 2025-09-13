package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventPoller implements Runnable {

    private final Consumer<Void, HubEventAvro> consumer;
    private final HubEventService hubEventService;

    @Override
    public void run() {
        log.info("Kafka Hub Event poller started");
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<Void, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Void, HubEventAvro> record : records) {
                    hubEventService.handleHubEvent(record.value());
                }
                consumer.commitAsync();
            }

        } catch (WakeupException e) {
            // break the cycle
        }
        log.info("Kafka Hub Event poller finished");
    }

}
