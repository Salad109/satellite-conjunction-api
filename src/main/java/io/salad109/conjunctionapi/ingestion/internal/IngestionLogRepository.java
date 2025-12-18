package io.salad109.conjunctionapi.ingestion.internal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IngestionLogRepository extends JpaRepository<IngestionLog, Long> {
}
