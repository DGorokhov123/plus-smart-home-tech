package ru.yandex.practicum.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dal.ShoppingCart;
import ru.yandex.practicum.dal.ShoppingCartRepository;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.cart.NoActiveShoppingCartException;
import ru.yandex.practicum.exception.cart.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.warehouse.ProductInShoppingCartLowQuantityInWarehouseException;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {

    private final ShoppingCartRepository shoppingCartRepository;
    private final WarehouseClient warehouseClient;

    // Добавить товар в корзину.
    @Transactional(readOnly = false)
    public ShoppingCartDto addProductsToCart(String username, Map<String, Long> addProductMap) {
        List<ShoppingCart> activeCarts = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
        ShoppingCart cart;
        if (activeCarts.isEmpty()) {
            cart = new ShoppingCart();
            cart.setUsername(username);
            cart.setIsActive(true);
            cart.setProducts(addProductMap);
            cart.setCreatedAt(Instant.now());
        } else {
            cart = activeCarts.getFirst();
            for (Map.Entry<String, Long> addProduct : addProductMap.entrySet()) {
                if (cart.getProducts().containsKey(addProduct.getKey())) {
                    Long sum = cart.getProducts().get(addProduct.getKey()) + addProduct.getValue();
                    cart.getProducts().put(addProduct.getKey(), sum);
                } else {
                    cart.getProducts().put(addProduct.getKey(), addProduct.getValue());
                }
            }
        }
        shoppingCartRepository.save(cart);
        try {
            warehouseClient.checkShoppingCart(ShoppingCartMapper.toDto(cart));
        } catch (FeignException.UnprocessableEntity e) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(e.getMessage());
        }
        return ShoppingCartMapper.toDto(cart);
    }

    // Удалить указанные товары из корзины пользователя.
    @Transactional(readOnly = false)
    public ShoppingCartDto removeProductsFromCart(String username, List<String> removeProductList) {
        List<ShoppingCart> activeCarts = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);

        if (activeCarts.isEmpty())
            throw new NoActiveShoppingCartException("No active shopping carts are found for user = " + username);

        ShoppingCart cart = activeCarts.getFirst();
        boolean noProductsInCart = removeProductList.stream()
                .anyMatch(p -> !cart.getProducts().containsKey(p));

        if (noProductsInCart) {
            String errorMsg = removeProductList.stream()
                    .filter(p -> !cart.getProducts().containsKey(p))
                    .map(p -> p + "; ")
                    .reduce(new StringBuilder("No products in cart: "), StringBuilder::append, StringBuilder::append)
                    .toString();
            throw new NoProductsInShoppingCartException(errorMsg);
        }

        for (String removeProductId : removeProductList) cart.getProducts().remove(removeProductId);
        shoppingCartRepository.save(cart);
        return ShoppingCartMapper.toDto(cart);
    }

    // Изменить количество товаров в корзине.
    @Transactional(readOnly = false)
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        List<ShoppingCart> activeCarts = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
        if (activeCarts.isEmpty())
            throw new NoActiveShoppingCartException("No active shopping carts are found for user = " + username);
        ShoppingCart cart = activeCarts.getFirst();

        boolean noProductInCart = !cart.getProducts().containsKey(request.getProductId());
        if (noProductInCart) {
            throw new NoProductsInShoppingCartException("No product in cart: " + request.getProductId());
        }

        cart.getProducts().put(request.getProductId(), request.getNewQuantity());
        shoppingCartRepository.save(cart);
        try {
            warehouseClient.checkShoppingCart(ShoppingCartMapper.toDto(cart));
        } catch (FeignException.UnprocessableEntity e) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(e.getMessage());
        }
        return ShoppingCartMapper.toDto(cart);
    }

    // Деактивация корзины товаров для пользователя.
    @Transactional(readOnly = false)
    public String deactivateCart(String username) {
        shoppingCartRepository.deactivateByUsername(username);
        return "true";
    }

    // Получить актуальную корзину для авторизованного пользователя.
    @Transactional(readOnly = false)
    public ShoppingCartDto getCartByUsername(String username) {
        List<ShoppingCart> activeCarts = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
        ShoppingCart cart;
        if (activeCarts.isEmpty()) {
            cart = new ShoppingCart();
            cart.setUsername(username);
            cart.setIsActive(true);
            cart.setCreatedAt(Instant.now());
            shoppingCartRepository.save(cart);
        } else {
            cart = activeCarts.getFirst();
        }
        return ShoppingCartMapper.toDto(cart);
    }

}
