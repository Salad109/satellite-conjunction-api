package io.salad109.conjunctionapi.api;

import io.salad109.conjunctionapi.ingestion.IngestionService;
import io.salad109.conjunctionapi.ingestion.SyncResult;
import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1")
public class CatalogController {

    private final IngestionService ingestionService;
    private final SatelliteRepository satelliteRepository;

    public CatalogController(IngestionService ingestionService, SatelliteRepository satelliteRepository) {
        this.ingestionService = ingestionService;
        this.satelliteRepository = satelliteRepository;
    }

    /**
     * Trigger a full catalog sync from Space-Track.
     */
    @PostMapping("/catalog/sync")
    public ResponseEntity<SyncResult> sync() {
        var result = ingestionService.sync();
        return result.successful() ?
                ResponseEntity.ok(result) :
                ResponseEntity.status(500).body(result);
    }

    /**
     * Get catalog statistics.
     */
    @GetMapping("/catalog/stats")
    public ResponseEntity<?> getStats() {
        long totalObjects = satelliteRepository.count();
        return ResponseEntity.ok(Map.of(
                "totalObjects", totalObjects,
                "timestamp", OffsetDateTime.now()
        ));
    }

    /**
     * Get a specific satellite by NORAD ID.
     */
    @GetMapping("/catalog/{noradId}")
    public ResponseEntity<?> getSatellite(@PathVariable Integer noradId) {
        Optional<Satellite> satelliteOpt = satelliteRepository.findById(noradId);
        if (satelliteOpt.isEmpty())
            return ResponseEntity.notFound().build();
        else
            return ResponseEntity.ok(satelliteOpt);
    }
}
