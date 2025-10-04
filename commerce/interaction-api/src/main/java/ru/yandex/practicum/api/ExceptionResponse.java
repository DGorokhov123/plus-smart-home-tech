package ru.yandex.practicum.api;

import lombok.Data;

@Data
public class ExceptionResponse {

    private String httpStatus;

    private String userMessage;

    private String message;

    private String localizedMessage;

    private Throwable cause;

    private StackTraceElement[] stackTrace = new StackTraceElement[0];

    private Throwable[] suppressed = new Throwable[0];

    public static ExceptionResponse from(Throwable e, String httpStatus) {
        ExceptionResponse response = new ExceptionResponse();
        response.setHttpStatus(httpStatus);
        response.setUserMessage(e.getMessage());
        response.setMessage(e.getMessage());
        response.setLocalizedMessage(e.getLocalizedMessage());
        response.setCause(e.getCause());
        response.setStackTrace(e.getStackTrace());
        response.setSuppressed(e.getSuppressed());
        return response;
    }

}
