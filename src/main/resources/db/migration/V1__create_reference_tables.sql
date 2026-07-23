CREATE TABLE projects (
    id UUID PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_inbox BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX ux_projects_single_inbox ON projects (is_inbox) WHERE is_inbox;

CREATE TABLE tags (
    id UUID PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    color VARCHAR(7),
    CONSTRAINT ck_tags_color CHECK (color IS NULL OR color ~ '^#[0-9A-Fa-f]{6}$')
);

CREATE TABLE recurrence_rules (
    id UUID PRIMARY KEY,
    rrule_string VARCHAR(1000) NOT NULL,
    freq VARCHAR(16) NOT NULL,
    recurrence_interval INTEGER NOT NULL DEFAULT 1,
    by_day VARCHAR(255),
    by_month_day INTEGER,
    by_month INTEGER,
    by_set_pos INTEGER,
    occurrence_count INTEGER,
    until_at TIMESTAMP,
    strategy VARCHAR(32) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_recurrence_freq CHECK (freq IN ('DAILY','WEEKLY','MONTHLY','YEARLY')),
    CONSTRAINT ck_recurrence_interval CHECK (recurrence_interval > 0),
    CONSTRAINT ck_recurrence_month_day CHECK (by_month_day IS NULL OR by_month_day = -1 OR by_month_day BETWEEN 1 AND 31),
    CONSTRAINT ck_recurrence_month CHECK (by_month IS NULL OR by_month BETWEEN 1 AND 12),
    CONSTRAINT ck_recurrence_count CHECK (occurrence_count IS NULL OR occurrence_count > 0),
    CONSTRAINT ck_recurrence_limit CHECK (occurrence_count IS NULL OR until_at IS NULL),
    CONSTRAINT ck_recurrence_strategy CHECK (strategy IN ('FIXED_SCHEDULE','COMPLETION_BASED'))
);

CREATE TABLE recurrence_exceptions (
    id UUID PRIMARY KEY,
    recurrence_rule_id UUID NOT NULL REFERENCES recurrence_rules(id) ON DELETE CASCADE,
    exception_date DATE NOT NULL,
    reason VARCHAR(16) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_recurrence_exception UNIQUE (recurrence_rule_id, exception_date),
    CONSTRAINT ck_recurrence_exception_reason CHECK (reason IN ('SKIPPED','RESCHEDULED'))
);
