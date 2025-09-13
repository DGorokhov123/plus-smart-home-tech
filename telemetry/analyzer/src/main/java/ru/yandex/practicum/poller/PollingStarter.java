package ru.yandex.practicum.poller;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

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

}
