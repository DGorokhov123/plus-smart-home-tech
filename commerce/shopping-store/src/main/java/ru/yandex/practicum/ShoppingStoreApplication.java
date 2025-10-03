package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@EnableCaching
@SpringBootApplication
public class ShoppingStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShoppingStoreApplication.class, args);
    }

}
