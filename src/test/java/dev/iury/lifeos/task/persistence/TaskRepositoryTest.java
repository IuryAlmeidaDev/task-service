package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.iury.lifeos.task.application.PageResult;
import dev.iury.lifeos.task.application.TaskFilter;
import dev.iury.lifeos.task.domain.model.Project;
import dev.iury.lifeos.task.domain.model.Task;
import dev.iury.lifeos.task.domain.model.TaskPriority;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class TaskRepositoryTest {

    @Inject
    TaskRepository tasks;

    @Inject
    ProjectRepository projects;

    @Test
    @TestTransaction
    void filtersVisibleActiveTasksAndHidesFutureStart() {
        Project inbox = new Project("Inbox", null, true);
        projects.persist(inbox);
        Task visible = Task.create("visível", inbox.id);
        visible.priority = TaskPriority.P1;
        Task future = Task.create("futura", inbox.id);
        future.priority = TaskPriority.P1;
        future.startDate = LocalDateTime.of(2026, 7, 23, 10, 0);
        tasks.persist(visible, future);

        PageResult<Task> result = tasks.findActive(
                new TaskFilter(null, TaskPriority.P1, null, null, null, null),
                Instant.parse("2026-07-22T13:00:00Z"), 0, 20);

        assertEquals(List.of("visível"), result.items().stream().map(task -> task.title).toList());
        assertEquals(1, result.total());
    }
}
