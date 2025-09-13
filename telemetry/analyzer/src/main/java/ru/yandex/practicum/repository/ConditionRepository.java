package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;

import java.util.List;

public interface ConditionRepository extends JpaRepository<Condition, Long> {

    List<Condition> findBySensorId(String sensorId);

    List<Condition> findBySensorIdAndScenarioHubId(String sensorId, String hubId);

    List<Condition> findByScenario(Scenario scenario);
}
