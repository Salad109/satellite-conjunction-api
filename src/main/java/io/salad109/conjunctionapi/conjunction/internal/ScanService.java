package io.salad109.conjunctionapi.conjunction.internal;

import io.salad109.conjunctionapi.satellite.SatellitePair;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.orekit.utils.PVCoordinates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ScanService {

    private static final Logger log = LoggerFactory.getLogger(ScanService.class);

    private final PropagationService propagationService;

    public ScanService(PropagationService propagationService) {
        this.propagationService = propagationService;
    }

    public List<Conjunction> scanForConjunctions(List<SatellitePair> pairs, Map<Integer, TLEPropagator> propagators, double toleranceKm, double thresholdKm, int lookaheadHours, int stepSeconds) {
        log.debug("Starting conjunction scan for {} pairs over {} hours (tolerance={} km, threshold={} km)",
                pairs.size(), lookaheadHours, toleranceKm, thresholdKm);
        // Coarse sweep
        List<CoarseDetection> coarseDetections = coarseSweep(pairs, propagators, OffsetDateTime.now(ZoneOffset.UTC), toleranceKm, stepSeconds, lookaheadHours);
        log.info("Coarse sweep found {} detections", coarseDetections.size());

        if (coarseDetections.isEmpty()) {
            log.warn("No close approaches detected in lookahead window");
            return List.of();
        }

        // Group into events
        Map<SatellitePair, List<List<CoarseDetection>>> eventsByPair = groupIntoEvents(coarseDetections, stepSeconds);

        // Refine each event
        // Flatten for parallel processing
        List<List<CoarseDetection>> allEvents = eventsByPair.values().stream()
                .flatMap(List::stream)
                .toList();

        log.debug("Refining {} events...", allEvents.size());
        long refineStartMs = System.currentTimeMillis();

        // Refine and filter by threshold
        List<Conjunction> conjunctionsUnderThreshold = allEvents.parallelStream()
                .map(event -> refineEvent(event, propagators, stepSeconds))
                .filter(refined -> refined.getMissDistanceKm() <= thresholdKm)
                .toList();

        log.info("Refined to {} conjunctions below {} km threshold in {}ms",
                conjunctionsUnderThreshold.size(), thresholdKm, System.currentTimeMillis() - refineStartMs);

        // Deduplicate by pair, keeping the closest approach
        List<Conjunction> deduplicated = conjunctionsUnderThreshold.stream()
                .collect(Collectors.toMap(
                        c -> c.getObject1NoradId() + ":" + c.getObject2NoradId(),
                        c -> c,
                        (a, b) -> a.getMissDistanceKm() <= b.getMissDistanceKm() ? a : b
                ))
                .values()
                .stream()
                .toList();

        log.debug("Deduplicated to {} unique pairs", deduplicated.size());
        return deduplicated;
    }

    /**
     * Scan through lookahead window in large steps and record all detections within toleranceKm.
     */
    List<CoarseDetection> coarseSweep(List<SatellitePair> pairs, Map<Integer, TLEPropagator> propagators,
                                      OffsetDateTime startTime, double toleranceKm, int stepSeconds, int lookaheadHours) {
        long startMs = System.currentTimeMillis();

        int totalSteps = (lookaheadHours * 3600) / stepSeconds;
        int logInterval = Math.max(1, totalSteps / 10); // Log every 10%
        log.debug("Coarse sweep: {} steps over {} hours at {}s intervals", totalSteps, lookaheadHours, stepSeconds);

        int stepCount = 0;
        List<CoarseDetection> detections = new ArrayList<>();
        for (int offsetSeconds = 0; offsetSeconds <= lookaheadHours * 3600; offsetSeconds += stepSeconds) {
            OffsetDateTime time = startTime.plusSeconds(offsetSeconds);
            Map<Integer, PVCoordinates> positions = propagationService.propagateAll(propagators, time);

            List<CoarseDetection> stepDetections = pairs.parallelStream()
                    .filter(pair -> {
                        PVCoordinates pvA = positions.get(pair.a().getNoradCatId());
                        PVCoordinates pvB = positions.get(pair.b().getNoradCatId());
                        if (pvA == null || pvB == null) return false;
                        return propagationService.calculateDistance(pvA, pvB) < toleranceKm;
                    })
                    .map(pair -> {
                        PVCoordinates pvA = positions.get(pair.a().getNoradCatId());
                        PVCoordinates pvB = positions.get(pair.b().getNoradCatId());
                        return new CoarseDetection(pair, time, propagationService.calculateDistance(pvA, pvB));
                    })
                    .toList();

            detections.addAll(stepDetections);
            stepCount++;

            if (stepCount % logInterval == 0) {
                int percent = (stepCount * 100) / totalSteps;
                log.debug("Coarse sweep progress: {}% ({}/{} steps)",
                        percent, stepCount, totalSteps);
            }
        }

        log.debug("Coarse sweep completed in {}ms with {} total detections",
                System.currentTimeMillis() - startMs, detections.size());
        return detections;
    }

    /**
     * Group detections by pair, then cluster consecutive detections into events (orbital passes).
     * Two detections belong to the same event if they're within 3 steps of each other.
     */
    Map<SatellitePair, List<List<CoarseDetection>>> groupIntoEvents(List<CoarseDetection> detections, int stepSeconds) {
        // Group by pair
        Map<SatellitePair, List<CoarseDetection>> byPair = detections.stream()
                .collect(Collectors.groupingBy(CoarseDetection::pair));

        // Cluster each pair's detections by time gap
        int gapThresholdSeconds = stepSeconds * 3;

        Map<SatellitePair, List<List<CoarseDetection>>> result = new HashMap<>();
        for (var entry : byPair.entrySet()) {
            List<CoarseDetection> sorted = entry.getValue().stream()
                    .sorted(Comparator.comparing(CoarseDetection::time))
                    .toList();

            List<List<CoarseDetection>> events = clusterByTimeGap(sorted, gapThresholdSeconds);
            result.put(entry.getKey(), events);
        }

        log.debug("Grouped into {} events across {} pairs",
                result.values().stream().mapToInt(List::size).sum(), result.size());

        return result;
    }

    /**
     * Cluster sorted detections into groups where consecutive items are within gapThresholdSeconds.
     */
    private List<List<CoarseDetection>> clusterByTimeGap(List<CoarseDetection> sorted, int gapThresholdSeconds) {
        List<List<CoarseDetection>> clusters = new ArrayList<>();
        if (sorted.isEmpty()) return clusters;

        List<CoarseDetection> currentCluster = new ArrayList<>();
        currentCluster.add(sorted.getFirst());

        for (int i = 1; i < sorted.size(); i++) {
            CoarseDetection prev = sorted.get(i - 1);
            CoarseDetection curr = sorted.get(i);

            long gapSeconds = ChronoUnit.SECONDS.between(prev.time(), curr.time());

            if (gapSeconds <= gapThresholdSeconds) {
                currentCluster.add(curr);
            } else {
                clusters.add(currentCluster);
                currentCluster = new ArrayList<>();
                currentCluster.add(curr);
            }
        }
        clusters.add(currentCluster);

        return clusters;
    }

    /**
     * Refine an event (cluster of coarse detections) using binary search to find more accurate TCA and minimum distance.
     */
    Conjunction refineEvent(List<CoarseDetection> event, Map<Integer, TLEPropagator> propagators, int stepSeconds) {
        // Start with the detection that had minimum distance
        CoarseDetection best = event.stream()
                .min(Comparator.comparing(CoarseDetection::distance))
                .orElseThrow();

        SatellitePair pair = best.pair();
        OffsetDateTime tca = best.time();
        double minDistance = best.distance();

        // Use binary search to narrow down to 100ms accuracy
        long searchWindowMs = stepSeconds * 1000L;
        while (searchWindowMs >= 100) {
            searchWindowMs /= 2;

            OffsetDateTime left = tca.minusNanos(searchWindowMs * 1_000_000);
            OffsetDateTime right = tca.plusNanos(searchWindowMs * 1_000_000);

            double distLeft = propagationService.propagateAndMeasureDistance(pair, propagators, left);
            double distRight = propagationService.propagateAndMeasureDistance(pair, propagators, right);

            if (distLeft < minDistance && distLeft <= distRight) {
                tca = left;
                minDistance = distLeft;
            } else if (distRight < minDistance) {
                tca = right;
                minDistance = distRight;
            } // else keep current tca and retry with smaller window
        }

        // Calculate relative velocity at refined TCA
        double relativeVelocity = propagationService.propagateAndMeasureVelocity(pair, propagators, tca);

        return new Conjunction(
                null,
                pair.a().getNoradCatId(),
                pair.b().getNoradCatId(),
                minDistance,
                tca,
                relativeVelocity
        );
    }

    record CoarseDetection(SatellitePair pair, OffsetDateTime time, double distance) {
    }
}
