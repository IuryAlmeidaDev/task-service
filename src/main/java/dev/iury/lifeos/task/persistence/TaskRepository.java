package dev.iury.lifeos.task.persistence;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import dev.iury.lifeos.task.application.PageResult;
import dev.iury.lifeos.task.application.TaskFilter;
import dev.iury.lifeos.task.domain.model.Task;
import dev.iury.lifeos.task.domain.model.TaskStatus;
import io.quarkus.hibernate.orm.panache.PanacheQuery;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import io.quarkus.panache.common.Sort;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TaskRepository implements PanacheRepositoryBase<Task, UUID> {

    public PageResult<Task> findActive(TaskFilter filter, Instant now, int page, int size) {
        StringBuilder hql = new StringBuilder(
                "deletedAt is null and (startDate is null or startDate <= function('timezone', timezone, :now))");
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("now", now);
        if (filter.status() != null) {
            hql.append(" and status = :status");
            parameters.put("status", filter.status());
        }
        if (filter.priority() != null) {
            hql.append(" and priority = :priority");
            parameters.put("priority", filter.priority());
        }
        if (filter.projectId() != null) {
            hql.append(" and projectId = :projectId");
            parameters.put("projectId", filter.projectId());
        }
        if (filter.dueFrom() != null) {
            hql.append(" and dueDate >= :dueFrom");
            parameters.put("dueFrom", filter.dueFrom());
        }
        if (filter.dueTo() != null) {
            hql.append(" and dueDate <= :dueTo");
            parameters.put("dueTo", filter.dueTo());
        }
        if (filter.tagId() != null) {
            hql.append(" and id in (select tt.id.taskId from TaskTag tt where tt.id.tagId = :tagId)");
            parameters.put("tagId", filter.tagId());
        }

        PanacheQuery<Task> query = find(
                hql.toString(),
                Sort.by("dueDate", Sort.Direction.Ascending, Sort.NullPrecedence.NULLS_LAST)
                        .and("createdAt", Sort.Direction.Descending),
                parameters);
        long total = query.count();
        List<Task> items = query.page(Page.of(page, size)).list();
        return new PageResult<>(items, total, page, size);
    }

    public Optional<Task> findActiveById(UUID id) {
        return find("id = ?1 and deletedAt is null", id).firstResultOptional();
    }

    public List<Task> listTrash() {
        return list("deletedAt is not null", Sort.by("deletedAt", Sort.Direction.Descending));
    }

    public List<Task> listOverdue() {
        return list("deletedAt is null and status = ?1", TaskStatus.OVERDUE);
    }

    public List<Task> listChildren(UUID parentId) {
        return list("parentTaskId = ?1 and deletedAt is null", parentId);
    }

    public long countSeries(UUID rootId) {
        return count("id = ?1 or parentRecurringTaskId = ?1", rootId);
    }

    public long hardDeleteBefore(Instant cutoff) {
        return delete("deletedAt is not null and deletedAt < function('timezone', timezone, ?1)", cutoff);
    }
}
