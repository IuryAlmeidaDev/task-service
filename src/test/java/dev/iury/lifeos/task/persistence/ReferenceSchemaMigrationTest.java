package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
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

    @Test
    void rejectsSecondInboxProject() throws Exception {
        inTransaction(connection -> {
            insertProject(connection, "Inbox A", true);

            assertThrows(SQLException.class, () -> insertProject(connection, "Inbox B", true));
        });
    }

    @Test
    void rejectsInvalidTagColor() throws Exception {
        inTransaction(connection -> assertThrows(SQLException.class, () -> {
            try (var statement = connection.prepareStatement(
                    "insert into tags (id, name, color) values (?, ?, ?)")) {
                statement.setObject(1, UUID.randomUUID());
                statement.setString(2, "invalid-color");
                statement.setString(3, "red");
                statement.executeUpdate();
            }
        }));
    }

    @Test
    void rejectsZeroRecurrenceInterval() throws Exception {
        inTransaction(connection -> assertThrows(SQLException.class,
                () -> insertRecurrenceRule(connection, 0, null, null)));
    }

    @Test
    void rejectsRecurrenceCountCombinedWithUntil() throws Exception {
        inTransaction(connection -> assertThrows(SQLException.class,
                () -> insertRecurrenceRule(connection, 1, 2, "2030-01-01 00:00:00")));
    }

    @Test
    void rejectsDuplicateRecurrenceException() throws Exception {
        inTransaction(connection -> {
            UUID ruleId = insertRecurrenceRule(connection, 1, null, null);
            insertRecurrenceException(connection, ruleId);

            assertThrows(SQLException.class, () -> insertRecurrenceException(connection, ruleId));
        });
    }

    @Test
    void deletingRecurrenceRuleCascadesToException() throws Exception {
        inTransaction(connection -> {
            UUID ruleId = insertRecurrenceRule(connection, 1, null, null);
            insertRecurrenceException(connection, ruleId);

            try (var statement = connection.prepareStatement("delete from recurrence_rules where id = ?")) {
                statement.setObject(1, ruleId);
                assertEquals(1, statement.executeUpdate());
            }

            try (var statement = connection.prepareStatement(
                    "select count(*) from recurrence_exceptions where recurrence_rule_id = ?")) {
                statement.setObject(1, ruleId);
                try (ResultSet result = statement.executeQuery()) {
                    result.next();
                    assertEquals(0, result.getInt(1));
                }
            }
        });
    }

    @Test
    void inboxUniqueIndexIsPartial() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select indexdef from pg_indexes
                        where schemaname = 'public'
                          and indexname = 'ux_projects_single_inbox'
                        """);
                ResultSet result = statement.executeQuery()) {
            assertTrue(result.next(), "ux_projects_single_inbox must exist");
            String normalizedDefinition = result.getString(1).toLowerCase().replaceAll("\\s+", " ");
            assertTrue(normalizedDefinition.matches(".*where \\(?is_inbox\\)?$"), normalizedDefinition);
        }
    }

    private void inTransaction(SqlWork work) throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                work.execute(connection);
            } finally {
                connection.rollback();
            }
        }
    }

    private void insertProject(Connection connection, String name, boolean inbox) throws SQLException {
        try (var statement = connection.prepareStatement(
                "insert into projects (id, name, is_inbox) values (?, ?, ?)")) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, name);
            statement.setBoolean(3, inbox);
            statement.executeUpdate();
        }
    }

    private UUID insertRecurrenceRule(
            Connection connection, int interval, Integer count, String until) throws SQLException {
        UUID id = UUID.randomUUID();
        try (var statement = connection.prepareStatement("""
                insert into recurrence_rules
                    (id, rrule_string, freq, recurrence_interval, occurrence_count,
                     until_at, strategy, timezone)
                values (?, 'FREQ=DAILY', 'DAILY', ?, ?, ?::timestamp,
                        'FIXED_SCHEDULE', 'America/Cuiaba')
                """)) {
            statement.setObject(1, id);
            statement.setInt(2, interval);
            statement.setObject(3, count);
            statement.setString(4, until);
            statement.executeUpdate();
        }
        return id;
    }

    private void insertRecurrenceException(Connection connection, UUID ruleId) throws SQLException {
        try (var statement = connection.prepareStatement("""
                insert into recurrence_exceptions
                    (id, recurrence_rule_id, exception_date, reason)
                values (?, ?, date '2030-01-01', 'SKIPPED')
                """)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setObject(2, ruleId);
            statement.executeUpdate();
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void execute(Connection connection) throws Exception;
    }
}
