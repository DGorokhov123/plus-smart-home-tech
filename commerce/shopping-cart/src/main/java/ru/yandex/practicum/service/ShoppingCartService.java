package ru.yandex.practicum.service;

import feign.FeignException;
import jakarta.persistence.OptimisticLockException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dal.ShoppingCart;
import ru.yandex.practicum.dal.ShoppingCartRepository;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.cart.NoActiveShoppingCartException;
import ru.yandex.practicum.exception.cart.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.warehouse.ProductInShoppingCartLowQuantityInWarehouseException;
import ru.yandex.practicum.logging.Logging;

import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {

    private final ShoppingCartRepository shoppingCartRepository;
    private final WarehouseClient warehouseClient;

    private final TransactionTemplate transactionTemplate;

    // Добавить товар в корзину.
    @Logging
    @Retryable(
            retryFor = {OptimisticLockException.class, ConcurrentModificationException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2)
    )
    public ShoppingCartDto addProductsToCart(String username, Map<String, Long> addProductMap) {
        ShoppingCartDto checkCartDto = new ShoppingCartDto();
        checkCartDto.setShoppingCartId("no-matter-but-not-blank");
        List<ShoppingCart> activeCarts1 = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);

        boolean savedIsEmpty = activeCarts1.isEmpty();
        Long savedVersion = savedIsEmpty ? null : activeCarts1.getFirst().getVersion();
        String savedShoppingCartId = savedIsEmpty ? null : activeCarts1.getFirst().getShoppingCartId();

        if (activeCarts1.isEmpty()) {
            checkCartDto.setProducts(addProductMap);
        } else {
            Map<String, Long> allProductMap = Stream.concat(
                    activeCarts1.getFirst().getProducts().entrySet().stream(),
                    addProductMap.entrySet().stream()
            ).collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    Long::sum
            ));
            checkCartDto.setProducts(allProductMap);
        }

        try {
            warehouseClient.checkShoppingCart(checkCartDto);
        } catch (FeignException.UnprocessableEntity e) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(e.getMessage());
        }

        return transactionTemplate.execute(status -> {
            List<ShoppingCart> activeCarts2 = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
            ShoppingCart cart;
            if (activeCarts2.isEmpty()) {
                if (!savedIsEmpty)
                    throw new ConcurrentModificationException("Active cart deleted during method execution");
                cart = new ShoppingCart();
                cart.setUsername(username);
                cart.setIsActive(true);
                cart.setProducts(addProductMap);
                cart.setCreatedAt(Instant.now());
            } else {
                cart = activeCarts2.getFirst();
                if (savedIsEmpty || !Objects.equals(savedVersion, cart.getVersion())
                        || !Objects.equals(savedShoppingCartId, cart.getShoppingCartId()))
                    throw new ConcurrentModificationException("Active cart changed during method execution");
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
            return ShoppingCartMapper.toDto(cart);
        });
    }

    // Удалить указанные товары из корзины пользователя.
    @Logging
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
    @Logging
    @Retryable(
            retryFor = {OptimisticLockException.class, ConcurrentModificationException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2)
    )
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        List<ShoppingCart> activeCarts1 = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
        if (activeCarts1.isEmpty())
            throw new NoActiveShoppingCartException("No active shopping carts are found for user = " + username);
        ShoppingCart cart1 = activeCarts1.getFirst();

        if (!cart1.getProducts().containsKey(request.getProductId()))
            throw new NoProductsInShoppingCartException("No product in cart: " + request.getProductId());

        Long savedVersion = cart1.getVersion();
        String savedShoppingCartId = cart1.getShoppingCartId();

        ShoppingCartDto checkCartDto = new ShoppingCartDto();
        checkCartDto.setShoppingCartId("no-matter-but-not-blank");
        checkCartDto.setProducts(Map.of(request.getProductId(), request.getNewQuantity()));
        try {
            warehouseClient.checkShoppingCart(checkCartDto);
        } catch (FeignException.UnprocessableEntity e) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(e.getMessage());
        }

        return transactionTemplate.execute(status -> {
            List<ShoppingCart> activeCarts2 = shoppingCartRepository.findByUsernameAndIsActiveOrderByCreatedAtDesc(username, true);
            if (activeCarts2.isEmpty())
                throw new ConcurrentModificationException("Active cart deleted during method execution");
            ShoppingCart cart2 = activeCarts2.getFirst();
            if (!Objects.equals(savedVersion, cart2.getVersion())
                    || !Objects.equals(savedShoppingCartId, cart2.getShoppingCartId()))
                throw new ConcurrentModificationException("Active cart changed during method execution");
            cart2.getProducts().put(request.getProductId(), request.getNewQuantity());
            shoppingCartRepository.save(cart2);
            return ShoppingCartMapper.toDto(cart2);
        });
    }

    // Деактивация корзины товаров для пользователя.
    @Logging
    @Transactional(readOnly = false)
    public String deactivateCart(String username) {
        shoppingCartRepository.deactivateByUsername(username);
        return "true";
    }

    // Получить актуальную корзину для авторизованного пользователя.
    @Logging
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
