package io.salad109.conjunctionapi.ingestion;

import java.time.OffsetDateTime;

public record SyncResult(
        OffsetDateTime startedAt,
        int objectsInserted,
        int objectsUpdated,
        int objectsSkipped,
        int objectsDeleted,
        boolean successful
) {
    public static SyncResult failed(OffsetDateTime startedAt) {
        return new SyncResult(startedAt, 0, 0, 0, 0, false);
    }
}