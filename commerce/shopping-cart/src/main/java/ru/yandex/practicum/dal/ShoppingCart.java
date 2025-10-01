package ru.yandex.practicum.dal;

import jakarta.persistence.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Entity
@Table(name = "carts")
public class ShoppingCart {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @EqualsAndHashCode.Include
    @Column(name = "cart_id", nullable = false, length = 50)
    private String shoppingCartId;

    @Column(name = "username", nullable = false, length = 50)
    private String username;

    @Column(name = "is_active", nullable = false)
    private Boolean isActive;

    @ElementCollection
    @CollectionTable(name = "cart_products", joinColumns = @JoinColumn(name = "cart_id"))
    @MapKeyColumn(name = "product_id", nullable = false, length = 50)
    @Column(name = "quantity", nullable = false)
    private Map<String, Long> products = new HashMap<>();

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

}
