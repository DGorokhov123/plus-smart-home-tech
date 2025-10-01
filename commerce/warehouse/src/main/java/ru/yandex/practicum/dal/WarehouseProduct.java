package ru.yandex.practicum.dal;

import jakarta.persistence.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Entity
@Table(name = "warehouse_products")
public class WarehouseProduct {

    @Id
    @EqualsAndHashCode.Include
    @Column(name = "product_id", nullable = false, length = 50)
    private String productId;

    @Column(name = "depth", nullable = false)
    private BigDecimal depth;

    @Column(name = "height", nullable = false)
    private BigDecimal height;

    @Column(name = "width", nullable = false)
    private BigDecimal width;

    @Column(name = "weight", nullable = false)
    private BigDecimal weight;

    @Column(name = "fragile", nullable = true)
    private Boolean fragile;

    @Column(name = "quantity", nullable = false)
    private Long quantity;

}
