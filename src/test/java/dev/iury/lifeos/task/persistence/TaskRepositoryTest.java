package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.iury.lifeos.task.application.PageResult;
import dev.iury.lifeos.task.application.TaskFilter;
import dev.iury.lifeos.task.domain.model.Project;
import dev.iury.lifeos.task.domain.model.Tag;
import dev.iury.lifeos.task.domain.model.Task;
import dev.iury.lifeos.task.domain.model.TaskPriority;
import dev.iury.lifeos.task.domain.model.TaskStatus;
import dev.iury.lifeos.task.domain.model.TaskTag;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.persistence.EntityManager;
import jakarta.inject.Inject;

@QuarkusTest
class TaskRepositoryTest {

    @Inject
    TaskRepository tasks;

    @Inject
    ProjectRepository projects;

    @Inject
    TagRepository tags;

    @Inject
    EntityManager entityManager;

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

    @Test
    @TestTransaction
    void listsOnlyActiveOverdueTasks() {
        Project inbox = new Project("Inbox", null, true);
        projects.persist(inbox);
        Task overdue = Task.create("atrasada", inbox.id);
        overdue.status = TaskStatus.OVERDUE;
        Task pending = Task.create("pendente", inbox.id);
        tasks.persist(overdue, pending);

        assertEquals(List.of("atrasada"), tasks.listOverdue().stream().map(task -> task.title).toList());
    }

    @Test
    @TestTransaction
    void combinesStatusProjectTagDueDateAndStartDateFilters() {
        Project inbox = project("Inbox");
        Project work = project("Trabalho");
        Tag urgent = new Tag("urgente", "#ff0000");
        tags.persist(urgent);

        Task matching = task("corresponde", inbox.id, TaskStatus.IN_PROGRESS, LocalDateTime.of(2026, 7, 22, 12, 0));
        matching.startDate = LocalDateTime.of(2026, 7, 22, 8, 0);
        Task wrongStatus = task("status", inbox.id, TaskStatus.TODO, matching.dueDate);
        Task wrongProject = task("projeto", work.id, TaskStatus.IN_PROGRESS, matching.dueDate);
        Task wrongTag = task("tag", inbox.id, TaskStatus.IN_PROGRESS, matching.dueDate);
        Task beforeRange = task("antes", inbox.id, TaskStatus.IN_PROGRESS, LocalDateTime.of(2026, 7, 21, 23, 59));
        Task afterRange = task("depois", inbox.id, TaskStatus.IN_PROGRESS, LocalDateTime.of(2026, 7, 23, 0, 1));
        Task future = task("futura", inbox.id, TaskStatus.IN_PROGRESS, LocalDateTime.of(2026, 7, 22, 15, 0));
        future.startDate = LocalDateTime.of(2026, 7, 22, 14, 0);
        tasks.persist(matching, wrongStatus, wrongProject, wrongTag, beforeRange, afterRange, future);
        entityManager.persist(new TaskTag(matching.id, urgent.id));
        entityManager.persist(new TaskTag(wrongStatus.id, urgent.id));
        entityManager.persist(new TaskTag(wrongProject.id, urgent.id));
        entityManager.persist(new TaskTag(beforeRange.id, urgent.id));
        entityManager.persist(new TaskTag(afterRange.id, urgent.id));
        entityManager.persist(new TaskTag(future.id, urgent.id));

        PageResult<Task> result = tasks.findActive(
                new TaskFilter(
                        TaskStatus.IN_PROGRESS,
                        null,
                        inbox.id,
                        urgent.id,
                        LocalDateTime.of(2026, 7, 22, 0, 0),
                        LocalDateTime.of(2026, 7, 23, 0, 0)),
                Instant.parse("2026-07-22T13:00:00Z"),
                0,
                20);

        assertEquals(List.of("corresponde"), result.items().stream().map(task -> task.title).toList());
        assertEquals(1, result.total());
    }

    @Test
    @TestTransaction
    void ordersDueDatesFirstAndPaginatesNullDueDatesByNewestCreation() {
        Project inbox = project("Inbox");
        Task early = task("prazo cedo", inbox.id, TaskStatus.TODO, LocalDateTime.of(2026, 7, 22, 9, 0));
        Task late = task("prazo tarde", inbox.id, TaskStatus.TODO, LocalDateTime.of(2026, 7, 22, 18, 0));
        Task nullOlder = task("sem prazo antiga", inbox.id, TaskStatus.TODO, null);
        nullOlder.createdAt = LocalDateTime.of(2026, 7, 20, 10, 0);
        Task nullNewer = task("sem prazo nova", inbox.id, TaskStatus.TODO, null);
        nullNewer.createdAt = LocalDateTime.of(2026, 7, 21, 10, 0);
        tasks.persist(early, late, nullOlder, nullNewer);
        TaskFilter filter = new TaskFilter(null, null, null, null, null, null);
        Instant now = Instant.parse("2026-07-22T13:00:00Z");

        PageResult<Task> firstPage = tasks.findActive(filter, now, 0, 2);
        PageResult<Task> secondPage = tasks.findActive(filter, now, 1, 2);

        assertEquals(List.of("prazo cedo", "prazo tarde"),
                firstPage.items().stream().map(task -> task.title).toList());
        assertEquals(List.of("sem prazo nova", "sem prazo antiga"),
                secondPage.items().stream().map(task -> task.title).toList());
        assertEquals(4, firstPage.total());
        assertEquals(0, firstPage.page());
        assertEquals(2, firstPage.size());
        assertEquals(4, secondPage.total());
        assertEquals(1, secondPage.page());
        assertEquals(2, secondPage.size());
    }

    @Test
    @TestTransaction
    void findsOnlyActiveTaskById() {
        Project inbox = project("Inbox");
        Task active = Task.create("ativa", inbox.id);
        Task deleted = Task.create("excluída", inbox.id);
        deleted.deletedAt = LocalDateTime.of(2026, 7, 22, 10, 0);
        tasks.persist(active, deleted);

        assertEquals(active.id, tasks.findActiveById(active.id).orElseThrow().id);
        assertTrue(tasks.findActiveById(deleted.id).isEmpty());
        assertTrue(tasks.findActiveById(UUID.randomUUID()).isEmpty());
    }

    @Test
    @TestTransaction
    void listsTrashByMostRecentDeletion() {
        Project inbox = project("Inbox");
        Task active = Task.create("ativa", inbox.id);
        Task older = Task.create("antiga", inbox.id);
        older.deletedAt = LocalDateTime.of(2026, 7, 20, 10, 0);
        Task newer = Task.create("nova", inbox.id);
        newer.deletedAt = LocalDateTime.of(2026, 7, 21, 10, 0);
        tasks.persist(active, older, newer);

        assertEquals(List.of("nova", "antiga"), tasks.listTrash().stream().map(task -> task.title).toList());
    }

    @Test
    @TestTransaction
    void listsOnlyActiveChildren() {
        Project inbox = project("Inbox");
        Task parent = Task.create("pai", inbox.id);
        tasks.persist(parent);
        Task child = Task.create("filha", inbox.id);
        child.parentTaskId = parent.id;
        Task deletedChild = Task.create("filha excluída", inbox.id);
        deletedChild.parentTaskId = parent.id;
        deletedChild.deletedAt = LocalDateTime.of(2026, 7, 22, 10, 0);
        Task other = Task.create("outra", inbox.id);
        tasks.persist(child, deletedChild, other);

        assertEquals(List.of("filha"), tasks.listChildren(parent.id).stream().map(task -> task.title).toList());
    }

    @Test
    @TestTransaction
    void countsRecurringMasterAndItsInstances() {
        Project inbox = project("Inbox");
        Task root = Task.create("raiz", inbox.id);
        tasks.persist(root);
        Task first = Task.create("primeira", inbox.id);
        first.parentRecurringTaskId = root.id;
        Task second = Task.create("segunda", inbox.id);
        second.parentRecurringTaskId = root.id;
        Task unrelated = Task.create("outra", inbox.id);
        tasks.persist(first, second, unrelated);

        assertEquals(3, tasks.countSeries(root.id));
    }

    @Test
    @TestTransaction
    void hardDeletesOnlyTrashOlderThanCutoff() {
        Project inbox = project("Inbox");
        Task active = Task.create("ativa", inbox.id);
        Task oldTrash = Task.create("lixeira antiga", inbox.id);
        oldTrash.deletedAt = LocalDateTime.of(2026, 7, 20, 10, 0);
        Task recentTrash = Task.create("lixeira recente", inbox.id);
        recentTrash.deletedAt = LocalDateTime.of(2026, 7, 22, 10, 0);
        tasks.persist(active, oldTrash, recentTrash);

        long deleted = tasks.hardDeleteBefore(Instant.parse("2026-07-21T13:00:00Z"));
        entityManager.clear();

        assertEquals(1, deleted);
        assertFalse(tasks.findByIdOptional(oldTrash.id).isPresent());
        assertTrue(tasks.findByIdOptional(active.id).isPresent());
        assertTrue(tasks.findByIdOptional(recentTrash.id).isPresent());
    }

    private Project project(String name) {
        Project project = new Project(name, null, false);
        projects.persist(project);
        return project;
    }

    private Task task(String title, UUID projectId, TaskStatus status, LocalDateTime dueDate) {
        Task task = Task.create(title, projectId);
        task.status = status;
        task.dueDate = dueDate;
        return task;
    }
}
