package io.salad109.conjunctionapi.ingestion;

import io.salad109.conjunctionapi.ingestion.internal.IngestionLog;
import io.salad109.conjunctionapi.ingestion.internal.IngestionLogRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Service
public class IngestionLogService {

    private final IngestionLogRepository ingestionLogRepository;

    public IngestionLogService(IngestionLogRepository ingestionLogRepository) {
        this.ingestionLogRepository = ingestionLogRepository;
    }

    @Transactional(readOnly = true)
    public Page<SyncResult> getSyncHistory(Pageable pageable) {
        Page<IngestionLog> page = ingestionLogRepository.findAllByOrderByStartedAtDesc(pageable);
        return page.map(log -> new SyncResult(
                log.getStartedAt(),
                log.getObjectsInserted(),
                log.getObjectsUpdated(),
                log.getObjectsSkipped(),
                log.getObjectsDeleted(),
                log.isSuccessful()
        ));
    }

    @Transactional(readOnly = true)
    public SyncResult getLatest() {
        IngestionLog log = ingestionLogRepository.findTopByOrderByStartedAtDesc();
        if (log == null)
            return null;
        else
            return new SyncResult(
                    log.getStartedAt(),
                    log.getObjectsInserted(),
                    log.getObjectsUpdated(),
                    log.getObjectsSkipped(),
                    log.getObjectsDeleted(),
                    log.isSuccessful()
            );
    }

    /**
     * Save an ingestion log entry in a new transaction. REQUIRES_NEW ensures log isn't rolled back if sync fails.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    void saveIngestionLog(SyncResult syncResult, String errorMessage) {
        ingestionLogRepository.save(new IngestionLog(
                null,
                syncResult.startedAt(),
                OffsetDateTime.now(ZoneOffset.UTC),
                syncResult.objectsInserted(),
                syncResult.objectsUpdated(),
                syncResult.objectsSkipped(),
                syncResult.objectsDeleted(),
                syncResult.successful(),
                errorMessage
        ));
    }
}
