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

    /**
     * Process OMM records - upsert satellites with their current TLE data.
     */
    private ProcessingResult processRecords(List<OmmRecord> records) {
        log.debug("Processing {} records...", records.size());
        int processed = 0;
        int created = 0;
        int updated = 0;
        int skipped = 0;
        int deleted;

        List<Integer> catalogIds = records.stream()
                .filter(OmmRecord::isValid)
                .map(OmmRecord::noradCatId)
                .distinct()
                .toList();

        // Delete satellites not in the new catalog
        deleted = satelliteRepository.deleteSatellitesByNoradCatIdNotIn(catalogIds);
        log.debug("Deleted {} satellites not present in the new catalog", deleted);

        // Load existing satellites
        Map<Integer, Satellite> existingSatellites = satelliteRepository
                .findAllById(catalogIds)
                .stream()
                .collect(Collectors.toMap(Satellite::getNoradCatId, Function.identity()));

        List<Satellite> toSave = new ArrayList<>();

        for (OmmRecord ommRecord : records) {
            if (!ommRecord.isValid()) {
                skipped++;
                continue;
            }

            Satellite satellite = existingSatellites.get(ommRecord.noradCatId());

            if (satellite == null) {
                // New satellite
                satellite = new Satellite(ommRecord.noradCatId());
                existingSatellites.put(ommRecord.noradCatId(), satellite);
                created++;
            } else {
                updated++;
            }

            // Update all fields from the record
            updateSatellite(satellite, ommRecord);
            toSave.add(satellite);
            processed++;

            // Batch save
            if (toSave.size() >= batchSize) {
                log.debug("Saving batch of {} satellites", toSave.size());
                satelliteRepository.saveAll(toSave);
                toSave.clear();
            }
        }

        // Save remaining
        if (!toSave.isEmpty()) {
            satelliteRepository.saveAll(toSave);
            log.debug("Saving batch of {} satellites", toSave.size());
        }

        log.debug("Processing complete: {} processed, {} created, {} updated, {} skipped, {} deleted",
                processed, created, updated, skipped, deleted);
        return new ProcessingResult(processed, created, updated, skipped, deleted);
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

        // Compute derived parameters (perigee, apogee, etc.)
        sat.computeDerivedParameters();
    }

    private record ProcessingResult(int processed, int created, int updated, int skipped, int deleted) {
    }
}
