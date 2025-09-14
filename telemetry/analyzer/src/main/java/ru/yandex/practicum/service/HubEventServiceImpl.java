package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.Sensor;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.mapper.EnumMapper;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventServiceImpl implements HubEventService {

    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    @Transactional(readOnly = false)
    public void handleHubEvent(HubEventAvro event) {
        log.debug("Received Hub Event: {}", event);
        if (event == null || event.getHubId() == null || event.getTimestamp() == null || event.getPayload() == null) {
            log.warn("Event was ignored. Incorrect fields: {}", event);
            return;
        }
        switch (event.getPayload()) {
            case DeviceAddedEventAvro payload:
                addDevice(payload, event.getHubId());
                break;
            case DeviceRemovedEventAvro payload:
                removeDevice(payload, event.getHubId());
                break;
            case ScenarioAddedEventAvro payload:
                addScenario(payload, event.getHubId());
                break;
            case ScenarioRemovedEventAvro payload:
                removeScenario(payload, event.getHubId());
                break;
            default:
                throw new IllegalArgumentException("Unsupported Hub Event Type: " + event.getPayload().getClass().getName());
        }
    }

    // SCENARIO

    private void addScenario(ScenarioAddedEventAvro payload, String hubId) {
        if (payload == null || payload.getName() == null || payload.getConditions() == null || payload.getActions() == null) {
            log.warn("Payload was ignored. Incorrect fields: {}", payload);
            return;
        }

        Scenario scenario = scenarioRepository.findByNameAndHubId(payload.getName(), hubId).orElseGet(() -> {
            Scenario s = new Scenario();
            s.setHubId(hubId);
            s.setName(payload.getName());
            return s;
        });

        List<Condition> conditions = payload.getConditions().stream()
                .map(c -> {
                    Integer value = switch (c.getValue()) {
                        case Boolean b -> b ? 1 : 0;
                        case Integer i -> i;
                        default -> null;
                    };
                    Condition condition = new Condition();
                    condition.setSensorId(c.getSensorId());
                    condition.setScenario(scenario);
                    condition.setType(EnumMapper.fromAvro(c.getType()));
                    condition.setOperation(EnumMapper.fromAvro(c.getOperation()));
                    condition.setValue(value);
                    return condition;
                })
                .toList();
        scenario.setConditions(conditions);

        List<Action> actions = payload.getActions().stream()
                .map(a -> {
                    Action action = new Action();
                    action.setSensorId(a.getSensorId());
                    action.setScenario(scenario);
                    action.setType(EnumMapper.fromAvro(a.getType()));
                    action.setValue(a.getValue());
                    return action;
                })
                .toList();
        scenario.setActions(actions);

        scenarioRepository.save(scenario);
        log.debug("Added scenario {} to hub {}", scenario, hubId);
    }

    private void removeScenario(ScenarioRemovedEventAvro payload, String hubId) {
        if (payload == null || payload.getName() == null) {
            log.warn("Payload was ignored. Incorrect fields: {}", payload);
            return;
        }
        if (!scenarioRepository.existsByNameAndHubId(payload.getName(), hubId)) {
            log.warn("Not found scenario {} on hub {}", payload.getName(), hubId);
            return;
        }
        scenarioRepository.deleteByNameAndHubId(payload.getName(), hubId);
        log.debug("Removed scenario {} from hub {}", payload.getName(), hubId);
    }

    // DEVICE

    private void addDevice(DeviceAddedEventAvro payload, String hubId) {
        if (payload == null || payload.getId() == null || payload.getType() == null) {
            log.warn("Payload was ignored. Incorrect fields: {}", payload);
            return;
        }
        if (sensorRepository.existsById(payload.getId())) {
            log.warn("Unable to add device with duplicate id {}", payload.getId());
            return;
        }
        Sensor sensor = new Sensor();
        sensor.setId(payload.getId());
        sensor.setHubId(hubId);
        sensor.setType(EnumMapper.fromAvro(payload.getType()));
        sensorRepository.save(sensor);
        log.debug("Added sensor {} to hub {}", sensor, hubId);
    }

    private void removeDevice(DeviceRemovedEventAvro payload, String hubId) {
        if (payload == null || payload.getId() == null) {
            log.warn("Payload was ignored. Incorrect fields: {}", payload);
            return;
        }
        if (!sensorRepository.existsById(payload.getId())) {
            log.warn("Not found device {} on hub {}", payload.getId(), hubId);
            return;
        }
        sensorRepository.deleteById(payload.getId());
        log.debug("Removed sensor {} from hub {}", payload.getId(), hubId);
    }

}
