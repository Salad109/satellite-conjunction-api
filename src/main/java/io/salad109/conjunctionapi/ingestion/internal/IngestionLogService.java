package io.salad109.conjunctionapi.ingestion.internal;

import io.salad109.conjunctionapi.ingestion.SyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Service
public class IngestionLogService {

    private final IngestionLogRepository ingestionLogRepository;

    public IngestionLogService(IngestionLogRepository ingestionLogRepository) {
        this.ingestionLogRepository = ingestionLogRepository;
    }

    /**
     * Save an ingestion log entry in a new transaction. REQUIRES_NEW ensures log isn't rolled back if sync fails.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveIngestionLog(SyncResult syncResult, String errorMessage) {
        ingestionLogRepository.save(new IngestionLog(
                null,
                syncResult.startedAt(),
                OffsetDateTime.now(),
                syncResult.objectsProcessed(),
                syncResult.objectsInserted(),
                syncResult.objectsUpdated(),
                syncResult.objectsSkipped(),
                syncResult.objectsDeleted(),
                syncResult.successful(),
                errorMessage
        ));
    }
}
