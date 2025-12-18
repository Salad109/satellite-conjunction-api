package io.salad109.conjunctionapi.ingestion;

import io.salad109.conjunctionapi.ingestion.internal.IngestionLogService;
import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteRepository;
import io.salad109.conjunctionapi.spacetrack.OmmRecord;
import io.salad109.conjunctionapi.spacetrack.SpaceTrackClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final SpaceTrackClient spaceTrackClient;
    private final SatelliteRepository satelliteRepository;
    private final IngestionLogService ingestionLogService;

    @Value("${ingestion.batch-size:1000}")
    private int batchSize;

    public IngestionService(SpaceTrackClient spaceTrackClient,
                            SatelliteRepository satelliteRepository,
                            IngestionLogService ingestionLogService) {
        this.spaceTrackClient = spaceTrackClient;
        this.satelliteRepository = satelliteRepository;
        this.ingestionLogService = ingestionLogService;
    }

    /**
     * Perform a full catalog sync from Space-Track.
     */
    @Scheduled(cron = "${ingestion.schedule.cron:0 21 */6 * * *}")
    @Transactional
    public SyncResult sync() {
        log.info("Starting catalog sync...");
        long startTime = System.currentTimeMillis();
        OffsetDateTime startedAt = OffsetDateTime.now();

        try {
            List<OmmRecord> records = spaceTrackClient.fetchCatalog();
            ProcessingResult processingResult = processRecords(records);
            SyncResult syncResult = new SyncResult(startedAt, processingResult.processed(), processingResult.created(), processingResult.updated(), processingResult.skipped(), processingResult.deleted(), true);
            ingestionLogService.saveIngestionLog(syncResult, null);

            log.info("Sync completed in {}ms. {} processed, {} created, {} updated, {} skipped, {} deleted",
                    System.currentTimeMillis() - startTime, processingResult.processed(), processingResult.created(), processingResult.updated(), processingResult.skipped(), processingResult.deleted());

            return syncResult;
        } catch (IOException e) {
            SyncResult failedSyncResult = SyncResult.failed(startedAt);
            ingestionLogService.saveIngestionLog(failedSyncResult, e.getMessage());

            log.error("Failed synchronizing with Space-Track API", e);

            return failedSyncResult;
        }
    }

    private record ProcessingResult(int processed, int created, int updated, int skipped, int deleted) {
    }

    /**
     * Process OMM records - upsert satellites with their current TLE data.
     */
    private ProcessingResult processRecords(List<OmmRecord> records) {
        log.debug("Processing {} records...", records.size());

        // Filter to valid records only
        List<OmmRecord> validRecords = records.stream()
                .filter(OmmRecord::isValid)
                .toList();

        int skipped = records.size() - validRecords.size();
        log.debug("Filtered {} invalid records", skipped);

        // Extract catalog IDs
        List<Integer> catalogIds = validRecords.stream()
                .map(OmmRecord::noradCatId)
                .distinct()
                .toList();

        // Clean up removed satellites
        int deleted = satelliteRepository.deleteSatellitesByNoradCatIdNotIn(catalogIds);
        log.debug("Deleted {} satellites not present in the new catalog", deleted);

        // Load existing satellites for comparison
        Map<Integer, Satellite> existingById = satelliteRepository.findAllById(catalogIds).stream()
                .collect(Collectors.toMap(Satellite::getNoradCatId, Function.identity()));

        // 4. Categorize records
        List<Satellite> toCreate = new ArrayList<>();
        List<Satellite> toUpdate = new ArrayList<>();

        for (OmmRecord ommRecord : validRecords) {
            Satellite existing = existingById.get(ommRecord.noradCatId());

            if (existing == null) {
                // New satellite
                toCreate.add(createSatellite(ommRecord));
            } else if (hasChanged(existing, ommRecord)) {
                // Existing satellite with changes
                updateSatellite(existing, ommRecord);
                toUpdate.add(existing);
            } else {
                // Existing satellite without changes
                skipped++;
            }
        }

        // Persist changes
        int created = saveBatched(toCreate, "Created");
        int updated = saveBatched(toUpdate, "Updated");

        log.debug("Processing complete: {} created, {} updated, {} skipped, {} deleted",
                created, updated, skipped, deleted);

        return new ProcessingResult(created + updated, created, updated, skipped, deleted);
    }

    private void updateSatellite(Satellite sat, OmmRecord ommRecord) {
        // Metadata
        sat.setObjectName(ommRecord.objectName());
        sat.setObjectType(ommRecord.objectType());
        sat.setCountryCode(ommRecord.countryCode());
        sat.setLaunchDate(ommRecord.launchDate());
        sat.setDecayDate(ommRecord.decayDate());

        // TLE data
        sat.setEpoch(ommRecord.getEpochUtc());
        sat.setTleLine1(ommRecord.tleLine1());
        sat.setTleLine2(ommRecord.tleLine2());

        // Orbital elements
        sat.setMeanMotion(ommRecord.meanMotion());
        sat.setEccentricity(ommRecord.eccentricity());
        sat.setInclination(ommRecord.inclination());
        sat.setRaan(ommRecord.raan());
        sat.setArgPerigee(ommRecord.argPerigee());
        sat.setMeanAnomaly(ommRecord.meanAnomaly());
        sat.setBstar(ommRecord.bstar() != null ? ommRecord.bstar() : 0.0);

        // Compute derived parameters
        sat.computeDerivedParameters();
    }

    private Satellite createSatellite(OmmRecord ommRecord) {
        Satellite satellite = new Satellite(ommRecord.noradCatId());
        updateSatellite(satellite, ommRecord);
        return satellite;
    }

    private boolean hasChanged(Satellite satellite, OmmRecord ommRecord) {
        return satellite.getEpoch() == null || !satellite.getEpoch().equals(ommRecord.getEpochUtc());
    }

    private int saveBatched(List<Satellite> satellites, String operation) {
        if (satellites.isEmpty()) {
            return 0;
        }

        for (int i = 0; i < satellites.size(); i += batchSize) {
            List<Satellite> batch = satellites.subList(i, Math.min(i + batchSize, satellites.size()));
            satelliteRepository.saveAll(batch);
            satelliteRepository.flush();
            log.debug("{} batch of {} satellites", operation, batch.size());
        }

        return satellites.size();
    }
}
