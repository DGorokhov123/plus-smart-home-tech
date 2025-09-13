package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequestProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.mapper.EnumMapper;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SnapshotServiceImpl implements SnapshotService {

    private final Map<String, SpecificRecordBase> sensorStates = new HashMap<>();

    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub grpcClient;

    @Override
    public void handleSnapshot(SensorsSnapshotAvro snapshot) {
        log.debug("Received Snapshot: {}", snapshot);
        if (snapshot == null || snapshot.getHubId() == null || snapshot.getTimestamp() == null || snapshot.getSensorsState() == null) {
            log.warn("Snapshot was ignored. Incorrect fields: {}", snapshot);
            return;
        }
        for (Map.Entry<String, SensorStateAvro> state : snapshot.getSensorsState().entrySet()) {
            if (state.getValue().getData() != null && state.getValue().getData() instanceof SpecificRecordBase data) {
                handleData(data, snapshot.getHubId(), state.getKey());
            }
        }
    }

    private void handleData(SpecificRecordBase data, String hubId, String sensorId) {
        SpecificRecordBase currentState = sensorStates.get(sensorId);
        // Nothing to do if state:  1. In cache  2. Has not been changed
        if (currentState != null && Objects.equals(currentState, data)) return;
        sensorStates.put(sensorId, data);

        List<Condition> sensorConditions = conditionRepository.findBySensorIdAndScenarioHubId(sensorId, hubId);
        List<Condition> triggeredConditions = sensorConditions.stream()
                .filter(c -> ConditionChecker.matches(c, data))
                .toList();
        Set<Scenario> scenarios = triggeredConditions.stream()
                .map(Condition::getScenario)
                .collect(Collectors.toSet());
        List<Scenario> triggeredScenarios = scenarios.stream()
                .filter(s -> !s.getConditions().isEmpty())
                .filter(s -> s.getConditions().stream().allMatch(c -> ConditionChecker.matches(c, sensorStates.get(c.getSensorId()))))
                .toList();
        List<Action> actions = triggeredScenarios.stream()
                .map(Scenario::getActions)
                .flatMap(List::stream)
                .toList();

        actions.forEach(action -> sendAction(action, hubId));
    }

    private void sendAction(Action action, String hubId) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
        DeviceActionProto deviceAction = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(EnumMapper.toProto(action.getType()))
                .setValue(action.getValue())
                .build();
        DeviceActionRequestProto actionRequest = DeviceActionRequestProto.newBuilder()
                .setHubId(hubId)
                .setScenarioName(action.getScenario().getName())
                .setAction(deviceAction)
                .setTimestamp(timestamp)
                .build();
        grpcClient.handleDeviceAction(actionRequest);
        log.debug("Sent Action: {}", actionRequest);
    }

}
