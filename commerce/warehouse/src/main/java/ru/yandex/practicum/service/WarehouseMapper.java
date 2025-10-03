package ru.yandex.practicum.service;

import ru.yandex.practicum.dal.WarehouseProduct;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

public class WarehouseMapper {
    public static WarehouseProduct toNewEntity(NewProductInWarehouseRequest request) {
        WarehouseProduct result = new WarehouseProduct();
        result.setProductId(request.getProductId());
        result.setDepth(request.getDimension().getDepth());
        result.setHeight(request.getDimension().getHeight());
        result.setWidth(request.getDimension().getWidth());
        result.setWeight(request.getWeight());
        result.setFragile(request.getFragile());
        result.setQuantity(0L);
        return result;
    }

}
