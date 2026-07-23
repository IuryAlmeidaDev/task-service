package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.ResultSet;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ReferenceSchemaMigrationTest {
    @Inject
    AgroalDataSource dataSource;

    @Test
    void createsFourReferenceTables() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select count(*) from information_schema.tables
                        where table_schema = 'public'
                          and table_name in ('projects','tags','recurrence_rules','recurrence_exceptions')
                        """);
                ResultSet result = statement.executeQuery()) {
            result.next();
            assertEquals(4, result.getInt(1));
        }
    }
}
