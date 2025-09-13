package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Scenario;

import java.util.List;

public interface ActionRepository extends JpaRepository<Action, Long> {

    List<Action> findByScenario(Scenario scenario);

}
