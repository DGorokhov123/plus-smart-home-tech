package ru.yandex.practicum.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import ru.yandex.practicum.api.ExceptionResponse;
import ru.yandex.practicum.exception.store.ProductNotFoundException;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    // SPRING EXCEPTIONS

    @ExceptionHandler({
            ConstraintViolationException.class,              // Custom annotation exceptions
            MethodArgumentNotValidException.class            // @Valid annotation exceptions
    })
    public ResponseEntity<ExceptionResponse> handleConstraintViolation(Throwable e, HttpServletRequest request) {
        log.debug("VALIDATION FAILED: {}", e.getMessage());
        ExceptionResponse response = ExceptionResponse.from(e, HttpStatus.BAD_REQUEST.toString());
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler({
            IllegalArgumentException.class,                        // wrong arguments like -1
            MethodArgumentTypeMismatchException.class,             // argument type mismatch
            HttpMessageNotReadableException.class,                 // wrong json in request body
            MissingServletRequestParameterException.class          // missing RequestParam
    })
    public ResponseEntity<ExceptionResponse> handleIllegalArgument(Throwable e, HttpServletRequest request) {
        log.debug("ILLEGAL ARGUMENT: {}", e.getMessage());
        ExceptionResponse response = ExceptionResponse.from(e, HttpStatus.BAD_REQUEST.toString());
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    // CUSTOM EXCEPTIONS

    @ExceptionHandler(
            ProductNotFoundException.class                        // custom not_found exception
    )
    public ResponseEntity<ExceptionResponse> handleNotFoundException(ProductNotFoundException e, HttpServletRequest request) {
        log.debug("NOT FOUND: {}", e.getMessage());
        ExceptionResponse response = ExceptionResponse.from(e, HttpStatus.NOT_FOUND.toString());
        return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
    }

}
