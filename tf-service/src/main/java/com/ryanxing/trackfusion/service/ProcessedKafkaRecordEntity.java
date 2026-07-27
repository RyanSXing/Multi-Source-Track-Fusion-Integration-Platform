package com.ryanxing.trackfusion.service;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("processed_kafka_records")
public final class ProcessedKafkaRecordEntity implements Persistable<String> {
    @Id private String id;
    private String topic;
    private int partition;
    @Column("offset_value")
    private long offset;
    private Instant observedAt;
    private String outcome;
    private String detectionJson;
    @Transient private boolean newRecord;

    public ProcessedKafkaRecordEntity() {}

    public ProcessedKafkaRecordEntity(
            String id,
            String topic,
            int partition,
            long offset,
            Instant observedAt,
            String outcome,
            String detectionJson,
            boolean newRecord) {
        this.id = id;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.observedAt = observedAt;
        this.outcome = outcome;
        this.detectionJson = detectionJson;
        this.newRecord = newRecord;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return newRecord;
    }

    public String id() {
        return id;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Instant observedAt() {
        return observedAt;
    }

    public String outcome() {
        return outcome;
    }

    public String detectionJson() {
        return detectionJson;
    }
}
