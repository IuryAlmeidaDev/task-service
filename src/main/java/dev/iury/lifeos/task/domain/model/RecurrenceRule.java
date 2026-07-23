package dev.iury.lifeos.task.domain.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

@Entity
@Table(name = "recurrence_rules")
public class RecurrenceRule extends BaseEntity {

    @Column(name = "rrule_string", nullable = false, length = 1000)
    public String rruleString;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    public RecurrenceFrequency freq;

    @Column(name = "recurrence_interval", nullable = false)
    public int interval;

    @Column(name = "by_day")
    public String byDay;

    @Column(name = "by_month_day")
    public Integer byMonthDay;

    @Column(name = "by_month")
    public Integer byMonth;

    @Column(name = "by_set_pos")
    public Integer bySetPos;

    @Column(name = "occurrence_count")
    public Integer count;

    @Column(name = "until_at")
    public LocalDateTime until;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    public RecurrenceStrategy strategy;

    @Column(nullable = false, length = 64)
    public String timezone;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    public LocalDateTime updatedAt;

    protected RecurrenceRule() {
    }

    public RecurrenceRule(String rruleString, RecurrenceFrequency freq, int interval,
            String byDay, Integer byMonthDay, Integer byMonth, Integer bySetPos,
            Integer count, LocalDateTime until, RecurrenceStrategy strategy, String timezone) {
        this.rruleString = rruleString;
        this.freq = freq;
        this.interval = interval;
        this.byDay = byDay;
        this.byMonthDay = byMonthDay;
        this.byMonth = byMonth;
        this.bySetPos = bySetPos;
        this.count = count;
        this.until = until;
        this.strategy = strategy;
        this.timezone = timezone;
    }
}
