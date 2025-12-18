package io.salad109.conjunctionapi.ingestion.internal;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

@Entity
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "ingestion_log")
public class IngestionLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "started_at")
    private OffsetDateTime startedAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @Column(name = "objects_processed")
    private int objectsProcessed;

    @Column(name = "objects_inserted")
    private int objectsInserted;

    @Column(name = "objects_updated")
    private int objectsUpdated;

    @Column(name = "objects_skipped")
    private int objectsSkipped;

    @Column(name = "objects_deleted")
    private int objectsDeleted;

    @Column(name = "successful")
    private boolean successful;

    @Column(name = "error_message")
    private String errorMessage;
}
