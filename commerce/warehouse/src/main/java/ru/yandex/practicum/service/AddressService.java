package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.warehouse.AddressDto;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class AddressService {

    private final List<AddressDto> addresses = new ArrayList<>();
    private final Random random = new Random();

    public AddressService() {
        AddressDto addressDto1 = new AddressDto();
        addressDto1.setCountry("Russia");
        addressDto1.setCity("Moscow");
        addressDto1.setStreet("ul. Stroiteley");
        addressDto1.setHouse("25");
        addressDto1.setFlat("12");
        addresses.add(addressDto1);

        AddressDto addressDto2 = new AddressDto();
        addressDto2.setCountry("USA");
        addressDto2.setCity("New-York");
        addressDto2.setStreet("Wall Street");
        addressDto2.setHouse("1");
        addressDto2.setFlat("1");
        addresses.add(addressDto2);
    }

    public AddressDto getAddress() {
        return addresses.get(random.nextInt(addresses.size()));
    }

}
