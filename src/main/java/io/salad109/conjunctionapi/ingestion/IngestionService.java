package io.salad109.conjunctionapi.ingestion;

import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteRepository;
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
    private final IngestionLogRepository ingestionLogRepository;

    @Value("${ingestion.batch-size:1000}")
    private int batchSize;

    public IngestionService(SpaceTrackClient spaceTrackClient,
                            SatelliteRepository satelliteRepository,
                            IngestionLogRepository ingestionLogRepository) {
        this.spaceTrackClient = spaceTrackClient;
        this.satelliteRepository = satelliteRepository;
        this.ingestionLogRepository = ingestionLogRepository;
    }

    /**
     * Perform a full catalog sync from Space-Track.
     */
    @Transactional
    public IngestionResult fullSync() throws IOException {
        log.info("Starting full catalog sync...");
        long startTime = System.currentTimeMillis();
        OffsetDateTime startedAt = OffsetDateTime.now();

        List<OmmRecord> records = spaceTrackClient.fetchFullCatalog();
        IngestionResult result = processRecords(records);

        ingestionLogRepository.save(new IngestionLog(
                null,
                startedAt,
                OffsetDateTime.now(),
                result.processed,
                result.created,
                result.updated,
                result.skipped,
                true
        )); // todo handle unsuccessful checks

        long duration = System.currentTimeMillis() - startTime;
        log.info("Full sync completed in {}ms: {}", duration, result);

        return result;
    }

    /**
     * Incremental sync - fetch only TLEs updated since last sync.
     */
    @Scheduled(cron = "${ingestion.schedule.cron:0 21 */6 * * *}")
    @Transactional
    public IngestionResult incrementalSync() throws IOException {
        var logOptional = ingestionLogRepository.findTopBySuccessfulOrderByCompletedAtDesc(true);
        if (logOptional.isEmpty()) {
            log.info("No previous sync log found, performing full sync instead");
            return fullSync();
        }
        OffsetDateTime since = logOptional.get().getStartedAt();

        log.info("Starting incremental sync since {}...", since);
        long startTime = System.currentTimeMillis();
        OffsetDateTime startedAt = OffsetDateTime.now();

        List<OmmRecord> records = spaceTrackClient.fetchUpdatedSince(since.toInstant());
        IngestionResult result = processRecords(records);

        ingestionLogRepository.save(new IngestionLog(
                null,
                startedAt,
                OffsetDateTime.now(),
                result.processed,
                result.created,
                result.updated,
                result.skipped,
                true));

        long duration = System.currentTimeMillis() - startTime;
        log.info("Incremental sync completed in {}ms: {}", duration, result);

        return result;
    }

    /**
     * Process OMM records - upsert satellites with their current TLE data.
     */
    private IngestionResult processRecords(List<OmmRecord> records) {
        log.info("Processing {} records...", records.size());
        int processed = 0;
        int created = 0;
        int updated = 0;
        int skipped = 0;

        // Load existing satellites
        List<Integer> noradIds = records.stream()
                .filter(OmmRecord::isValid)
                .map(OmmRecord::noradCatId)
                .distinct()
                .toList();

        Map<Integer, Satellite> existingSatellites = satelliteRepository
                .findAllById(noradIds)
                .stream()
                .collect(Collectors.toMap(Satellite::getNoradCatId, Function.identity()));

        List<Satellite> toSave = new ArrayList<>();

        for (OmmRecord record : records) {
            if (!record.isValid()) {
                skipped++;
                continue;
            }

            Satellite satellite = existingSatellites.get(record.noradCatId());

            if (satellite == null) {
                // New satellite
                satellite = new Satellite(record.noradCatId());
                existingSatellites.put(record.noradCatId(), satellite);
                created++;
            } else {
                updated++;
            }

            // Update all fields from the record
            updateSatellite(satellite, record);
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
            log.info("Saved {} satellites", toSave.size());
        }

        return new IngestionResult(processed, created, updated, skipped);
    }

    private void updateSatellite(Satellite sat, OmmRecord record) {
        // Metadata
        sat.setObjectName(record.objectName());
        sat.setObjectType(record.objectType());
        sat.setCountryCode(record.countryCode());
        sat.setLaunchDate(record.launchDate());
        sat.setDecayDate(record.decayDate());

        // TLE data
        sat.setEpoch(record.getEpochUtc());
        sat.setTleLine1(record.tleLine1());
        sat.setTleLine2(record.tleLine2());

        // Orbital elements
        sat.setMeanMotion(record.meanMotion());
        sat.setEccentricity(record.eccentricity());
        sat.setInclination(record.inclination());
        sat.setRaan(record.raan());
        sat.setArgPerigee(record.argPerigee());
        sat.setMeanAnomaly(record.meanAnomaly());
        sat.setBstar(record.bstar() != null ? record.bstar() : 0.0);

        // Compute derived parameters (perigee, apogee, etc.)
        sat.computeDerivedParameters();
    }

    public record IngestionResult(int processed, int created, int updated, int skipped) {
    }
}
