package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.dal.Product;
import ru.yandex.practicum.dal.ProductRepository;
import ru.yandex.practicum.dto.store.*;
import ru.yandex.practicum.exception.store.ProductNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    // Создание нового товара в ассортименте
    public ProductDto createProduct(ProductDto productDto) {
        Product newProduct = ProductMapper.toNewEntity(productDto);
        productRepository.save(newProduct);
        return ProductMapper.toDto(newProduct);
    }

    // Обновление товара в ассортименте, например уточнение описания, характеристик и т.д.
    public ProductDto updateProduct(ProductDto productDto) {
        String productId = productDto.getProductId();
        if (productId == null) throw new IllegalArgumentException("Field 'productId' shouldn't be null");
        Product product = productRepository.findById(productId).orElseThrow(
                () -> new ProductNotFoundException("Product is not found, id = " + productId)
        );
        if (productDto.getProductName() != null && !Objects.equals(productDto.getProductName(), product.getProductName()))
            product.setProductName(productDto.getProductName());
        if (productDto.getDescription() != null && !Objects.equals(productDto.getDescription(), product.getDescription()))
            product.setDescription(productDto.getDescription());
        if (productDto.getImageSrc() != null && !Objects.equals(productDto.getImageSrc(), product.getImageSrc()))
            product.setImageSrc(productDto.getImageSrc());
        if (productDto.getQuantityState() != null && !Objects.equals(productDto.getQuantityState(), product.getQuantityState()))
            product.setQuantityState(productDto.getQuantityState());
        if (productDto.getProductState() != null && !Objects.equals(productDto.getProductState(), product.getProductState()))
            product.setProductState(productDto.getProductState());
        if (productDto.getProductCategory() != null && !Objects.equals(productDto.getProductCategory(), product.getProductCategory()))
            product.setProductCategory(productDto.getProductCategory());
        if (productDto.getPrice() != null && !Objects.equals(productDto.getPrice(), product.getPrice()))
            product.setPrice(productDto.getPrice());
        productRepository.save(product);
        return ProductMapper.toDto(product);
    }

    // Удалить товар из ассортимента магазина. Функция для менеджерского состава.
    public String removeProduct(String productId) {
        Product product = productRepository.findById(productId).orElseThrow(
                () -> new ProductNotFoundException("Product is not found, id = " + productId)
        );
        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        return "true";
    }

    // Установка статуса по товару. API вызывается со стороны склада.
    public String setQuantityState(String productId, QuantityState quantityState) {
        Product product = productRepository.findById(productId).orElseThrow(
                () -> new ProductNotFoundException("Product is not found, id = " + productId)
        );
        product.setQuantityState(quantityState);
        productRepository.save(product);
        return "true";
    }

    // Получить сведения по товару из БД.
    public ProductDto getById(String productId) {
        Product product = productRepository.findById(productId).orElseThrow(
                () -> new ProductNotFoundException("Product is not found, id = " + productId)
        );
        return ProductMapper.toDto(product);
    }

    // Получение списка товаров по типу в пагинированном виде
    public ProductCollectionDto getCollection(ProductCategory category, Integer page, Integer size, String sortString) {
        List<SortDto> sortList = parseSortString(sortString);
        Sort sort = sortList.stream()
                .map(s -> Sort.by(Sort.Direction.valueOf(s.getDirection()), s.getProperty()))
                .reduce(Sort.unsorted(), Sort::and);
        Pageable pageable = PageRequest.of(page, size, sort);

        Page<Product> products = productRepository.findAllByProductCategory(category, pageable);
        List<ProductDto> productDtos = products.stream()
                .map(ProductMapper::toDto)
                .toList();

        ProductCollectionDto result = new ProductCollectionDto();
        result.setContent(productDtos);
        result.setSort(sortList);
        return result;
    }

    // PRIVATE METHODS

    private List<SortDto> parseSortString(String input) {
        List<SortDto> result = new ArrayList<>();
        if (input == null || input.isEmpty()) return result;
        String[] parts = input.split(",");
        for (int i = 0; i < parts.length - 1; i += 2) {
            SortDto dto = new SortDto();
            dto.setProperty(parts[i].trim());
            dto.setDirection(parts[i + 1].trim());
            result.add(dto);
        }
        return result;
    }

}
