package ru.yandex.practicum.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import ru.yandex.practicum.exception.NotAuthorizedUserException;

public class ValidUsernameValidator implements ConstraintValidator<ValidUsername, String> {

    @Override
    public boolean isValid(String username, ConstraintValidatorContext constraintValidatorContext) {
        if (username == null) throw new NotAuthorizedUserException("Username shouldn't be null");

        if (username.isBlank()) throw new NotAuthorizedUserException("Username shouldn't be blank");

        if (!username.matches("^[a-zA-Z0-9_.-]+$")) {
            throw new NotAuthorizedUserException("Username contains invalid characters");
        }

        return true;
    }

}
