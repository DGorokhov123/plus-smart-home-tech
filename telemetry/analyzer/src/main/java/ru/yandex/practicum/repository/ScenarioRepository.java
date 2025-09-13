package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.entity.Scenario;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {

    Optional<Scenario> findByNameAndHubId(String name, String hubId);

    boolean existsByNameAndHubId(String name, String hubId);

    void deleteByNameAndHubId(String name, String hubId);

    List<Scenario> findByHubId(String hubId);

}
