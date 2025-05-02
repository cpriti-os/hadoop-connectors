package com.google.cloud.hadoop.util;

import com.google.common.flogger.GoogleLogger;
//import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.StringPayload;
import com.google.cloud.logging.Severity;
import java.util.Collections;

import java.time.Duration;
import java.util.Collections;

public class CustomLogger {

    private final GoogleLogger logger;

    // Private constructor to enforce singleton pattern
    private CustomLogger(Class<?> clazz) {
        this.logger = GoogleLogger.forEnclosingClass();
    }

    // Static factory method to create a logger for a specific class
    public static CustomLogger forClass(Class<?> clazz) {
        return new CustomLogger(clazz);
    }

    // Custom log methods
    public void atInfo(String message, Object... args) {
        sendToCloudLogging("INFO", message, args);
        logger.atInfo().log(message, args);
    }

    public void atWarning(String message, Object... args) {
        logger.atWarning().log(message, args);
    }

    public void atSevere(String message, Object... args) {
        logger.atSevere().log(message, args);
    }

    public void atFine(String message, Object... args) {
        logger.atFine().log(message, args);
    }

    public void atFinest(String message, Object... args) {
        logger.atFinest().log(message, args);
    }

    public void atConfig(String message, Object... args) {
        logger.atConfig().log(message, args);
    }

    public void withCause(Throwable cause, String message, Object... args) {
        logger.atSevere().withCause(cause).log(message, args);
    }
    private void sendToCloudLogging(String severity, String message, Object... args) {
        String formattedMessage = String.format(message, args);
        String logName = "gcs-connector";

        // Instantiates a client
        try (Logging logging = LoggingOptions.getDefaultInstance().getService()) {
            LogEntry entry =
                    LogEntry.newBuilder(StringPayload.of(formattedMessage))
                            .setSeverity(Severity.valueOf(severity))
                            .setLogName(logName)
//                            .setResource(MonitoredResource.newBuilder("global").build())
                            .build();

            // Writes the log entry asynchronously
            logging.write(Collections.singleton(entry));

            // Optional - flush any pending log entries just before Logging is closed
            logging.flush();
        }
        logger.atInfo.log("Logged: %s%n", formattedMessage);
    }
}