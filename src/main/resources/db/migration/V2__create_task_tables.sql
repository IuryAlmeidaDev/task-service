CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'TODO',
    priority VARCHAR(2) NOT NULL DEFAULT 'P4',
    start_date TIMESTAMP,
    due_date TIMESTAMP,
    due_time TIME,
    timezone VARCHAR(64) NOT NULL DEFAULT 'America/Sao_Paulo',
    estimated_duration INTEGER,
    requires_payment BOOLEAN NOT NULL DEFAULT FALSE,
    expected_amount NUMERIC(19,2),
    completion_event_published BOOLEAN NOT NULL DEFAULT FALSE,
    project_id UUID NOT NULL REFERENCES projects(id),
    parent_task_id UUID REFERENCES tasks(id),
    recurrence_rule_id UUID REFERENCES recurrence_rules(id),
    parent_recurring_task_id UUID REFERENCES tasks(id),
    is_recurring_master BOOLEAN NOT NULL DEFAULT FALSE,
    version INTEGER NOT NULL DEFAULT 1,
    completed_at TIMESTAMP,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_tasks_title CHECK (length(btrim(title)) BETWEEN 1 AND 255),
    CONSTRAINT ck_tasks_description CHECK (description IS NULL OR octet_length(description) <= 65536),
    CONSTRAINT ck_tasks_status CHECK (status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE')),
    CONSTRAINT ck_tasks_priority CHECK (priority IN ('P1','P2','P3','P4')),
    CONSTRAINT ck_tasks_dates CHECK (start_date IS NULL OR due_date IS NULL OR start_date <= due_date),
    CONSTRAINT ck_tasks_duration CHECK (estimated_duration IS NULL OR estimated_duration BETWEEN 1 AND 525600),
    CONSTRAINT ck_tasks_payment CHECK (requires_payment OR expected_amount IS NULL),
    CONSTRAINT ck_tasks_recurrence_due CHECK (recurrence_rule_id IS NULL OR due_date IS NOT NULL),
    CONSTRAINT ck_tasks_parent CHECK (parent_task_id IS NULL OR parent_task_id <> id)
);

CREATE TABLE task_tags (
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, tag_id)
);

CREATE TABLE task_dependencies (
    id UUID PRIMARY KEY,
    blocking_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocked_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    type VARCHAR(16) NOT NULL,
    CONSTRAINT uq_task_dependency UNIQUE (blocking_task_id, blocked_task_id),
    CONSTRAINT ck_dependency_self CHECK (blocking_task_id <> blocked_task_id),
    CONSTRAINT ck_dependency_type CHECK (type IN ('BLOCKS','RELATES_TO','DUPLICATES'))
);

CREATE TABLE activity_logs (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    from_status VARCHAR(20),
    to_status VARCHAR(20) NOT NULL,
    reason VARCHAR(1000),
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_activity_from_status CHECK (from_status IS NULL OR from_status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE')),
    CONSTRAINT ck_activity_to_status CHECK (to_status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE'))
);

CREATE INDEX ix_tasks_active_listing ON tasks (status, priority, project_id, due_date) WHERE deleted_at IS NULL;
CREATE INDEX ix_tasks_visible_start ON tasks (start_date) WHERE deleted_at IS NULL;
CREATE INDEX ix_tasks_parent ON tasks (parent_task_id);
CREATE INDEX ix_tasks_series ON tasks (parent_recurring_task_id);
CREATE INDEX ix_tasks_trash ON tasks (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX ix_task_dependencies_blocked ON task_dependencies (blocked_task_id, type);
CREATE INDEX ix_activity_logs_task_time ON activity_logs (task_id, occurred_at);
