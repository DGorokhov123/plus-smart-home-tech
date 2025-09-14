package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PollingStarter {

    private final ThreadPoolTaskExecutor executor;
    private final HubEventPoller hubEventPoller;
    private final SnapshotPoller snapshotPoller;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        executor.execute(hubEventPoller);
        executor.execute(snapshotPoller);
    }

    @EventListener(ContextClosedEvent.class)
    public void onApplicationShutdown(ContextClosedEvent event) {
        log.info("Application shutdown initiated, stopping pollers...");
        hubEventPoller.closeConsumer();
        snapshotPoller.closeConsumer();
    }

}
