package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TaskSchemaMigrationTest {
    @Inject
    AgroalDataSource dataSource;

    @Test
    void createsTaskTablesAndOptimisticVersion() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select count(*) from information_schema.columns
                        where table_schema='public' and table_name='tasks'
                          and column_name in ('id','status','version','deleted_at','completion_event_published')
                        """);
                var result = statement.executeQuery()) {
            result.next();
            assertEquals(5, result.getInt(1));
        }
    }

    @Test
    void versionAndCompletionEventHaveSafeDefaults() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select column_name, is_nullable, column_default
                        from information_schema.columns
                        where table_schema = 'public' and table_name = 'tasks'
                          and column_name in ('version', 'completion_event_published')
                        order by column_name
                        """);
                var result = statement.executeQuery()) {
            assertTrue(result.next());
            assertEquals("completion_event_published", result.getString("column_name"));
            assertEquals("NO", result.getString("is_nullable"));
            assertEquals("false", result.getString("column_default"));
            assertTrue(result.next());
            assertEquals("version", result.getString("column_name"));
            assertEquals("NO", result.getString("is_nullable"));
            assertEquals("1", result.getString("column_default"));
            assertFalse(result.next());
        }
    }

    @Test
    void relationForeignKeysCascadeOnTaskDeletion() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select conrelid::regclass::text as table_name,
                               a.attname as column_name,
                               confrelid::regclass::text as referenced_table,
                               confdeltype
                        from pg_constraint c
                        join lateral unnest(c.conkey) with ordinality as k(attnum, ord) on true
                        join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
                        where c.contype = 'f'
                          and c.connamespace = 'public'::regnamespace
                          and c.conrelid in (
                            'task_tags'::regclass,
                            'task_dependencies'::regclass,
                            'activity_logs'::regclass
                          )
                        order by table_name, column_name
                        """);
                var result = statement.executeQuery()) {
            assertForeignKey(result, "activity_logs", "task_id", "tasks", "c");
            assertForeignKey(result, "task_dependencies", "blocked_task_id", "tasks", "c");
            assertForeignKey(result, "task_dependencies", "blocking_task_id", "tasks", "c");
            assertForeignKey(result, "task_tags", "tag_id", "tags", "c");
            assertForeignKey(result, "task_tags", "task_id", "tasks", "c");
            assertFalse(result.next());
        }
    }

    @Test
    void deletingTaskCascadesRelationsAndActivity() throws Exception {
        inTransaction(connection -> {
            UUID projectId = insertProject(connection);
            UUID blockingTaskId = insertTask(connection, projectId);
            UUID blockedTaskId = insertTask(connection, projectId);
            UUID tagId = insertTag(connection);

            execute(connection, "insert into task_tags (task_id, tag_id) values (?, ?)",
                    blockingTaskId, tagId);
            execute(connection, """
                    insert into task_dependencies (id, blocking_task_id, blocked_task_id, type)
                    values (?, ?, ?, 'BLOCKS')
                    """, UUID.randomUUID(), blockingTaskId, blockedTaskId);
            execute(connection, """
                    insert into activity_logs (id, task_id, to_status)
                    values (?, ?, 'IN_PROGRESS')
                    """, UUID.randomUUID(), blockingTaskId);
            execute(connection, "delete from tasks where id = ?", blockingTaskId);

            assertEquals(0, count(connection, "task_tags"));
            assertEquals(0, count(connection, "task_dependencies"));
            assertEquals(0, count(connection, "activity_logs"));
            assertEquals(1, count(connection, "tags"));
        });
    }

    @Test
    void rejectsSelfDependency() throws Exception {
        inTransaction(connection -> {
            UUID taskId = insertTask(connection, insertProject(connection));

            assertThrows(SQLException.class, () -> execute(connection, """
                    insert into task_dependencies (id, blocking_task_id, blocked_task_id, type)
                    values (?, ?, ?, 'BLOCKS')
                    """, UUID.randomUUID(), taskId, taskId));
        });
    }

    @Test
    void createsListingAndJobIndexes() throws Exception {
        Set<String> expectedIndexes = Set.of(
                "ix_tasks_active_listing",
                "ix_tasks_visible_start",
                "ix_tasks_parent",
                "ix_tasks_series",
                "ix_tasks_trash",
                "ix_task_dependencies_blocked",
                "ix_activity_logs_task_time");
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select indexname from pg_indexes
                        where schemaname = 'public' and indexname = any (?)
                        """)) {
            statement.setArray(1, connection.createArrayOf("text", expectedIndexes.toArray()));
            try (var result = statement.executeQuery()) {
                var actualIndexes = new java.util.HashSet<String>();
                while (result.next()) {
                    actualIndexes.add(result.getString(1));
                }
                assertEquals(expectedIndexes, actualIndexes);
            }
        }
    }

    private void assertForeignKey(
            ResultSet result,
            String table,
            String column,
            String referencedTable,
            String deleteAction) throws SQLException {
        assertTrue(result.next(), table + "." + column + " foreign key must exist");
        assertEquals(table, result.getString("table_name"));
        assertEquals(column, result.getString("column_name"));
        assertEquals(referencedTable, result.getString("referenced_table"));
        assertEquals(deleteAction, result.getString("confdeltype"));
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

    private UUID insertProject(Connection connection) throws SQLException {
        UUID id = UUID.randomUUID();
        execute(connection, "insert into projects (id, name) values (?, ?)", id, "Project " + id);
        return id;
    }

    private UUID insertTag(Connection connection) throws SQLException {
        UUID id = UUID.randomUUID();
        execute(connection, "insert into tags (id, name) values (?, ?)", id, "tag-" + id);
        return id;
    }

    private UUID insertTask(Connection connection, UUID projectId) throws SQLException {
        UUID id = UUID.randomUUID();
        execute(connection, "insert into tasks (id, title, project_id) values (?, ?, ?)",
                id, "Task " + id, projectId);
        return id;
    }

    private void execute(Connection connection, String sql, Object... values) throws SQLException {
        try (var statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < values.length; index++) {
                statement.setObject(index + 1, values[index]);
            }
            statement.executeUpdate();
        }
    }

    private int count(Connection connection, String table) throws SQLException {
        try (var statement = connection.createStatement();
                var result = statement.executeQuery("select count(*) from " + table)) {
            result.next();
            return result.getInt(1);
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void execute(Connection connection) throws Exception;
    }
}
