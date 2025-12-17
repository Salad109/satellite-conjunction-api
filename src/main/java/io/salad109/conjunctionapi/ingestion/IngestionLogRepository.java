package io.salad109.conjunctionapi.ingestion;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IngestionLogRepository extends JpaRepository<IngestionLog, Long> {
    /**
     * Get most recent successful ingestion
     */
    Optional<IngestionLog> findTopBySuccessfulOrderByCompletedAtDesc(boolean successful);
}
