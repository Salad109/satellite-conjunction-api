package io.salad109.conjunctionapi.ingestion.internal;

import io.salad109.conjunctionapi.ingestion.IngestionLogService;
import io.salad109.conjunctionapi.ingestion.SyncResult;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/sync")
public class IngestionLogController {

    private final IngestionLogService ingestionLogService;

    public IngestionLogController(IngestionLogService ingestionLogService) {
        this.ingestionLogService = ingestionLogService;
    }

    @GetMapping("/history")
    public ResponseEntity<Page<SyncResult>> getSyncHistory(
            @PageableDefault(sort = "startedAt", direction = Sort.Direction.DESC) Pageable pageable) {
        return ResponseEntity.ok(ingestionLogService.getSyncHistory(pageable));
    }

    @GetMapping("/latest")
    public ResponseEntity<SyncResult> getLatestSync() {
        return ResponseEntity.ok(ingestionLogService.getLatest());
    }
}
