package io.salad109.conjunctionapi.ingestion;

import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteService;
import io.salad109.conjunctionapi.spacetrack.OmmRecord;
import io.salad109.conjunctionapi.spacetrack.SpaceTrackClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final SpaceTrackClient spaceTrackClient;
    private final SatelliteService satelliteService;
    private final IngestionLogService ingestionLogService;

    public IngestionService(SpaceTrackClient spaceTrackClient,
                            SatelliteService satelliteService,
                            IngestionLogService ingestionLogService) {
        this.spaceTrackClient = spaceTrackClient;
        this.satelliteService = satelliteService;
        this.ingestionLogService = ingestionLogService;
    }

    /**
     * Perform a full catalog sync from Space-Track.
     */
    @Transactional
    public void sync() {
        log.info("Starting catalog sync...");
        long startTime = System.currentTimeMillis();
        OffsetDateTime startedAt = OffsetDateTime.now();

        try {
            List<OmmRecord> records = spaceTrackClient.fetchCatalog();
            ProcessingResult processingResult = processRecords(records);
            SyncResult syncResult = new SyncResult(startedAt, processingResult.created(), processingResult.updated(), processingResult.skipped(), processingResult.deleted(), true);
            ingestionLogService.saveIngestionLog(syncResult, null);

            log.info("Sync completed in {}ms. {} created, {} updated, {} skipped, {} deleted",
                    System.currentTimeMillis() - startTime, processingResult.created(), processingResult.updated(), processingResult.skipped(), processingResult.deleted());
        } catch (IOException e) {
            SyncResult failedSyncResult = SyncResult.failed(startedAt);
            ingestionLogService.saveIngestionLog(failedSyncResult, e.getMessage());

            log.error("Failed synchronizing with Space-Track API", e);
        }
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
        int deleted = satelliteService.deleteByCatalogIdsNotIn(catalogIds);
        log.debug("Deleted {} satellites no longer in catalog", deleted);

        // Load existing satellites for comparison
        Map<Integer, Satellite> existingById = satelliteService.getByCatalogIds(catalogIds);

        // Categorize records
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
        int created = satelliteService.save(toCreate);
        log.debug("Created {} new satellites", created);
        int updated = satelliteService.save(toUpdate);
        log.debug("Updated {} existing satellites", updated);

        log.debug("Processing complete: {} created, {} updated, {} skipped, {} deleted",
                created, updated, skipped, deleted);

        return new ProcessingResult(created, updated, skipped, deleted);
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

    private record ProcessingResult(int created, int updated, int skipped, int deleted) {
    }
}
