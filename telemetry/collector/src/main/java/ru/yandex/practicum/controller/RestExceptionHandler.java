package ru.yandex.practicum.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;

@Slf4j
@RestControllerAdvice
@RequiredArgsConstructor
public class RestExceptionHandler {

    private final ObjectMapper mapper;

    // BAD_REQUEST ERRORS

    @ExceptionHandler(
            MethodArgumentNotValidException.class                 // @Valid annotation exceptions
    )
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handleMethodArgumentNotValidException(MethodArgumentNotValidException e, HttpServletRequest request) {
        String errorMessage = e.getBindingResult().getAllErrors().getFirst().getDefaultMessage();
        Object target = e.getBindingResult().getTarget();
        log.warn("VALIDATION FAILED: {} for {}", errorMessage, target);
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST)
                .error("Validation Failed")
                .message(errorMessage)
                .path(request.getRequestURI())
                .build();
        try {
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException ex) {
            return "Validation Failed: " + errorMessage + "\n" + ex.getMessage();
        }
    }

    @ExceptionHandler({
            ConstraintViolationException.class,                    // Custom annotation exceptions
            IllegalArgumentException.class,                        // wrong arguments like -1
            MethodArgumentTypeMismatchException.class,             // argument type mismatch
            HttpMessageNotReadableException.class,                 // wrong json in request body
            MissingServletRequestParameterException.class          // missing RequestParam
    })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handleIllegalArgument(Throwable e, HttpServletRequest request) {
        log.warn("ILLEGAL ARGUMENT: {}", e.getMessage());
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST)
                .error("Illegal Argument")
                .message(e.getMessage())
                .path(request.getRequestURI())
                .build();
        try {
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException ex) {
            return "Validation Failed: " + e.getMessage() + "\n" + ex.getMessage();
        }
    }


}
