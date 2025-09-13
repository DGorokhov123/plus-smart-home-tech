package ru.yandex.practicum.entity;

import jakarta.persistence.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Entity
@Table(name = "sensors")
public class Sensor {

    @Id
    @EqualsAndHashCode.Include
    @Column(name = "id", nullable = false)
    private String id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private DeviceType type;

}
