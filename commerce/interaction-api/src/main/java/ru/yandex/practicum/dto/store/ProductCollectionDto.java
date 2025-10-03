package ru.yandex.practicum.dto.store;

import lombok.Data;

import java.util.List;

@Data
public class ProductCollectionDto {

    private List<ProductDto> content;

    private List<SortDto> sort;

}
