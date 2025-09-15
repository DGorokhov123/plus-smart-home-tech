package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.entity.Sensor;

public interface SensorRepository extends JpaRepository<Sensor, String> {

//    boolean existsByIdAndHubId(String sensorId, String hubId);

}
