package ru.yandex.practicum.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ShoppingCartRepository extends JpaRepository<ShoppingCart, String> {

    List<ShoppingCart> findByUsernameAndIsActiveOrderByCreatedAtDesc(String username, boolean isActive);

    @Modifying
    @Query("""
            UPDATE ShoppingCart c
            SET c.isActive = false
            WHERE c.isActive = true
            AND c.username = :username
            """)
    int deactivateByUsername(
            @Param("username") String username
    );

}
