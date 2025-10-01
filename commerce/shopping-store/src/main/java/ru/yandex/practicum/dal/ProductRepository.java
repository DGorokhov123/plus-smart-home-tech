package ru.yandex.practicum.dal;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.dto.store.ProductCategory;
import ru.yandex.practicum.dto.store.ProductState;

public interface ProductRepository extends JpaRepository<Product, String> {

    Page<Product> findAllByProductCategory(
            ProductCategory category,
            Pageable pageable
    );

    Page<Product> findAllByProductCategoryAndProductState(
            ProductCategory category,
            ProductState state,
            Pageable pageable
    );

}
