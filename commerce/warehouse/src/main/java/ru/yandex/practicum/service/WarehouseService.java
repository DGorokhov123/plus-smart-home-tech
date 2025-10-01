package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dal.WarehouseProduct;
import ru.yandex.practicum.dal.WarehouseProductRepository;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.exception.warehouse.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.warehouse.ProductInShoppingCartLowQuantityInWarehouseException;
import ru.yandex.practicum.exception.warehouse.SpecifiedProductAlreadyInWarehouseException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class WarehouseService {

    private final WarehouseProductRepository warehouseProductRepository;
    private final AddressService addressService;

    // Добавить новый товар на склад.
    public String createProduct(NewProductInWarehouseRequest request) {
        if (warehouseProductRepository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException("Product already exists: id = " + request.getProductId());
        }
        WarehouseProduct newProduct = WarehouseMapper.toNewEntity(request);
        warehouseProductRepository.save(newProduct);
        return "true";
    }

    // Предварительно проверить что количество товаров на складе достаточно для данной корзины продуктов.
    public BookedProductsDto checkShoppingCart(ShoppingCartDto shoppingCartDto) {
        Map<String, Long> cartProductMap = shoppingCartDto.getProducts();
        List<WarehouseProduct> warehouseProducts = warehouseProductRepository.findAllById(cartProductMap.keySet());

        String lackMsg = "";
        String notEnoughMsg = "";
        boolean lackOfProducts = warehouseProducts.size() < cartProductMap.size();
        boolean notEnoughProducts = warehouseProducts.stream()
                .anyMatch(p -> p.getQuantity() < cartProductMap.get(p.getProductId()));
        if (lackOfProducts) {
            Set<String> lackProductIds = new HashSet<>(cartProductMap.keySet());
            for (WarehouseProduct wp : warehouseProducts) lackProductIds.remove(wp.getProductId());
            lackMsg = lackProductIds.stream()
                    .map(id -> id + "; ")
                    .reduce(new StringBuilder("Lack of products - "), StringBuilder::append, StringBuilder::append)
                    .toString();
        }
        if (notEnoughProducts) {
            notEnoughMsg = warehouseProducts.stream()
                    .filter(p -> p.getQuantity() < cartProductMap.get(p.getProductId()))
                    .map(p -> p.getProductId() + ": " + cartProductMap.get(p.getProductId()) + " in order, " + p.getQuantity() + " in stock; ")
                    .reduce(new StringBuilder("Not enough products - "), StringBuilder::append, StringBuilder::append)
                    .toString();
        }
        if (lackOfProducts || notEnoughProducts) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(lackMsg + notEnoughMsg);
        }

        BigDecimal deliveryWeight = warehouseProducts.stream()
                .map(WarehouseProduct::getWeight)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(2, RoundingMode.UP);

        BigDecimal deliveryVolume = warehouseProducts.stream()
                .map(
                        p -> BigDecimal.valueOf(cartProductMap.get(p.getProductId()))
                                .multiply(p.getWidth())
                                .multiply(p.getHeight())
                                .multiply(p.getDepth())
                )
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(2, RoundingMode.UP);

        Boolean fragile = warehouseProducts.stream()
                .map(WarehouseProduct::getFragile)
                .anyMatch(Boolean.TRUE::equals);

        BookedProductsDto bookedProductsDto = new BookedProductsDto();
        bookedProductsDto.setDeliveryWeight(deliveryWeight);
        bookedProductsDto.setDeliveryVolume(deliveryVolume);
        bookedProductsDto.setFragile(fragile);
        return bookedProductsDto;
    }

    // Принять товар на склад. (изменить кол-во товара)
    public String changeQuantity(AddProductToWarehouseRequest request) {
        WarehouseProduct product = warehouseProductRepository.findById(request.getProductId()).orElseThrow(
                () -> new NoSpecifiedProductInWarehouseException("Product is not found: id = " + request.getProductId())
        );
        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseProductRepository.save(product);
        return "true";
    }

    // Предоставить адрес склада для расчёта доставки.
    public AddressDto getAddress() {
        return addressService.getAddress();
    }

}
