package ru.yandex.practicum.logging;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Aspect
@Component
public class LoggingAspect {

    private final Logger logger = LoggerFactory.getLogger(LoggingAspect.class);

    @Before(value = "@annotation(ru.yandex.practicum.logging.Logging)")
    public void logBefore(JoinPoint joinPoint) {
        logger.debug(
                "{}.{}() [IN] - {}",
                joinPoint.getTarget().getClass().getSimpleName(),
                joinPoint.getSignature().getName(),
                Arrays.toString(joinPoint.getArgs())
        );
    }

    @AfterReturning(value = "@annotation(ru.yandex.practicum.logging.Logging)", returning = "result")
    public void logAfter(JoinPoint joinPoint, Object result) {
        logger.debug(
                "{}.{}() [OUT] - {}",
                joinPoint.getTarget().getClass().getSimpleName(),
                joinPoint.getSignature().getName(),
                result
        );
    }

    @AfterThrowing(value = "@annotation(ru.yandex.practicum.logging.Logging)", throwing = "exception")
    public void logException(JoinPoint joinPoint, Throwable exception) {
        logger.warn(
                "{}.{}() [EXCEPTION] - {}",
                joinPoint.getTarget().getClass().getSimpleName(),
                joinPoint.getSignature().getName(),
                exception.getMessage()
        );
    }

}
