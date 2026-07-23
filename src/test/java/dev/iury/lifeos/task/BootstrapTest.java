package dev.iury.lifeos.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class BootstrapTest {
    @Test
    void usesJava21AndTaskServiceName() {
        assertEquals("21", System.getProperty("java.specification.version"));
        assertEquals("task-service", ConfigProvider.getConfig()
                .getValue("quarkus.application.name", String.class));
        assertEquals(8081, ConfigProvider.getConfig().getValue("quarkus.http.port", Integer.class));
        assertEquals(Optional.of(true), ConfigProvider.getConfig()
                .getOptionalValue("quarkus.flyway.migrate-at-start", Boolean.class));
    }
}
