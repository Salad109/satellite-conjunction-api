package io.salad109.conjunctionapi.conjunction;

import io.salad109.conjunctionapi.conjunction.internal.Conjunction;
import io.salad109.conjunctionapi.conjunction.internal.ConjunctionInfo;
import io.salad109.conjunctionapi.conjunction.internal.ConjunctionRepository;
import io.salad109.conjunctionapi.satellite.PairReductionService;
import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatellitePair;
import io.salad109.conjunctionapi.satellite.SatelliteService;
import org.orekit.propagation.analytical.tle.TLE;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.orekit.time.AbsoluteDate;
import org.orekit.time.TimeScalesFactory;
import org.orekit.utils.PVCoordinates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ConjunctionService {

    private static final Logger log = LoggerFactory.getLogger(ConjunctionService.class);

    private final SatelliteService satelliteService;
    private final ConjunctionRepository conjunctionRepository;
    private final PairReductionService pairReductionService;

    @Value("${conjunction.tolerance-km:50.0}")
    private double toleranceKm;

    @Value("${conjunction.collision-threshold-km:5.0}")
    private double thresholdKm;

    @Value("${conjunction.lookahead-hours:24}")
    private int lookaheadHours;

    @Value("${conjunction.step-seconds:3}")
    private int stepSeconds;

    public ConjunctionService(SatelliteService satelliteService, ConjunctionRepository conjunctionRepository, PairReductionService pairReductionService) {
        this.satelliteService = satelliteService;
        this.conjunctionRepository = conjunctionRepository;
        this.pairReductionService = pairReductionService;
    }

    @Transactional(readOnly = true)
    public Page<ConjunctionInfo> getConjunctions(Pageable pageable, boolean includeFormations) {
        if (includeFormations) {
            return conjunctionRepository.getConjunctionInfosWithFormations(pageable);
        } else {
            return conjunctionRepository.getConjunctionInfos(pageable);
        }
    }

    @Transactional
    public void findConjunctions() {
        long startMs = System.currentTimeMillis();
        log.info("Starting conjunction screening...");

        // Load satellites and build propagators
        List<Satellite> satellites = satelliteService.getAll();
        log.debug("Loaded {} satellites", satellites.size());

        List<SatellitePair> pairs = pairReductionService.findPotentialCollisionPairs(satellites);
        log.debug("Reduced to {} candidate pairs", pairs.size());

        Map<Integer, TLEPropagator> propagators = buildPropagators(satellites);

        // Coarse sweep - collect all detections within toleranceKm
        List<CoarseDetection> allDetections = coarseSweep(pairs, propagators);
        log.info("Coarse sweep found {} detections", allDetections.size());

        if (allDetections.isEmpty()) {
            log.warn("No close approaches detected in lookahead window");
            return;
        }

        // Group detections by pair, then cluster into events (orbital passes)
        Map<SatellitePair, List<List<CoarseDetection>>> eventsByPair = groupIntoEvents(allDetections);
        int totalEvents = eventsByPair.values().stream().mapToInt(List::size).sum();
        log.debug("Grouped into {} events across {} pairs", totalEvents, eventsByPair.size());

        // Refine each event in parallel and filter by threshold
        log.info("Refining {} events...", totalEvents);
        long refineStartMs = System.currentTimeMillis();

        // Flatten all events into a single list for parallel processing
        List<List<CoarseDetection>> allEvents = eventsByPair.values().stream()
                .flatMap(List::stream)
                .toList();

        List<Conjunction> conjunctionsUnderThreshold = allEvents.parallelStream()
                .map(event -> refineEvent(event, propagators))
                .filter(refined -> refined.getMissDistanceKm() <= thresholdKm)
                .toList();

        log.info("Refined to {} conjunctions below {}km threshold in {}ms",
                conjunctionsUnderThreshold.size(), thresholdKm, System.currentTimeMillis() - refineStartMs);

        // Keep only closest approach per pair
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

        // Save all conjunctions (upsert keeps closest per pair)
        if (!deduplicated.isEmpty()) {
            conjunctionRepository.batchUpsertIfCloser(deduplicated);
        }

        log.info("Conjunction screening completed in {}ms", System.currentTimeMillis() - startMs);
    }

    /**
     * Scan through lookahead window in large steps and record all detections within toleranceKm.
     */
    public List<CoarseDetection> coarseSweep(List<SatellitePair> pairs, Map<Integer, TLEPropagator> propagators) {
        return coarseSweep(pairs, propagators, OffsetDateTime.now(ZoneOffset.UTC), toleranceKm, stepSeconds, lookaheadHours);
    }

    public List<CoarseDetection> coarseSweep(List<SatellitePair> pairs, Map<Integer, TLEPropagator> propagators,
                                             OffsetDateTime startTime, double toleranceKm, int stepSeconds, int lookaheadHours) {
        long startMs = System.currentTimeMillis();
        List<CoarseDetection> detections = new ArrayList<>();

        int totalSteps = (lookaheadHours * 3600) / stepSeconds;
        int logInterval = Math.max(1, totalSteps / 10); // Log every 10%
        log.debug("Coarse sweep: {} steps over {} hours at {}s intervals", totalSteps, lookaheadHours, stepSeconds);

        int stepCount = 0;
        for (int offsetSeconds = 0; offsetSeconds <= lookaheadHours * 3600; offsetSeconds += stepSeconds) {
            OffsetDateTime time = startTime.plusSeconds(offsetSeconds);
            Map<Integer, PVCoordinates> positions = propagateAll(propagators, time);

            List<CoarseDetection> stepDetections = pairs.parallelStream()
                    .filter(pair -> {
                        PVCoordinates pvA = positions.get(pair.a().getNoradCatId());
                        PVCoordinates pvB = positions.get(pair.b().getNoradCatId());
                        if (pvA == null || pvB == null) return false;
                        return calculateDistance(pvA, pvB) < toleranceKm;
                    })
                    .map(pair -> {
                        PVCoordinates pvA = positions.get(pair.a().getNoradCatId());
                        PVCoordinates pvB = positions.get(pair.b().getNoradCatId());
                        return new CoarseDetection(pair, time, calculateDistance(pvA, pvB));
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
    public Map<SatellitePair, List<List<CoarseDetection>>> groupIntoEvents(List<CoarseDetection> detections) {
        return groupIntoEvents(detections, stepSeconds);
    }

    public Map<SatellitePair, List<List<CoarseDetection>>> groupIntoEvents(List<CoarseDetection> detections, int stepSeconds) {
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

        return result;
    }

    /**
     * Cluster sorted detections into groups where consecutive items are within gapThresholdSeconds.
     */
    List<List<CoarseDetection>> clusterByTimeGap(List<CoarseDetection> sorted, int gapThresholdSeconds) {
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
    public Conjunction refineEvent(List<CoarseDetection> event, Map<Integer, TLEPropagator> propagators) {
        return refineEvent(event, propagators, stepSeconds);
    }

    public Conjunction refineEvent(List<CoarseDetection> event, Map<Integer, TLEPropagator> propagators, int stepSeconds) {
        // Start with the detection that had minimum distance
        CoarseDetection best = event.stream()
                .min(Comparator.comparing(CoarseDetection::distance))
                .orElseThrow();

        SatellitePair pair = best.pair();
        OffsetDateTime tca = best.time();
        double minDistance = best.distance();

        // Use binary search to narrow down to 100ms minimum
        long searchWindowMs = stepSeconds * 1000L;
        while (searchWindowMs >= 100) {
            searchWindowMs /= 2;

            OffsetDateTime left = tca.minusNanos(searchWindowMs * 1_000_000);
            OffsetDateTime right = tca.plusNanos(searchWindowMs * 1_000_000);

            double distLeft = propagateAndMeasure(pair, propagators, left);
            double distRight = propagateAndMeasure(pair, propagators, right);

            if (distLeft < minDistance && distLeft <= distRight) {
                tca = left;
                minDistance = distLeft;
            } else if (distRight < minDistance) {
                tca = right;
                minDistance = distRight;
            } // else keep current tca and retry with smaller window
        }

        // Calculate relative velocity at refined TCA
        double relativeVelocity = propagateAndMeasureVelocity(pair, propagators, tca); // todo unnecessary repropagating

        return new Conjunction(
                null,
                pair.a().getNoradCatId(),
                pair.b().getNoradCatId(),
                minDistance,
                tca,
                relativeVelocity
        );
    }

    /**
     * Propagate both satellites to a given time and return the distance between them.
     */
    double propagateAndMeasure(SatellitePair pair, Map<Integer, TLEPropagator> propagators, OffsetDateTime time) {
        AbsoluteDate date = toAbsoluteDate(time);

        try {
            TLEPropagator propA = propagators.get(pair.a().getNoradCatId());
            TLEPropagator propB = propagators.get(pair.b().getNoradCatId());

            PVCoordinates pvA = propA.getPVCoordinates(date, propA.getFrame());
            PVCoordinates pvB = propB.getPVCoordinates(date, propB.getFrame());

            return calculateDistance(pvA, pvB);
        } catch (Exception e) {
            log.warn("Failed to propagate for refinement: {}", e.getMessage());
            return Double.MAX_VALUE;
        }
    }

    /**
     * Propagate both satellites to a given time and return the relative velocity.
     */
    double propagateAndMeasureVelocity(SatellitePair pair, Map<Integer, TLEPropagator> propagators, OffsetDateTime time) {
        AbsoluteDate date = toAbsoluteDate(time);

        try {
            TLEPropagator propA = propagators.get(pair.a().getNoradCatId());
            TLEPropagator propB = propagators.get(pair.b().getNoradCatId());

            PVCoordinates pvA = propA.getPVCoordinates(date, propA.getFrame());
            PVCoordinates pvB = propB.getPVCoordinates(date, propB.getFrame());

            return calculateRelativeVelocity(pvA, pvB);
        } catch (Exception e) {
            log.warn("Failed to calculate velocity: {}", e.getMessage());
            return 0.0;
        }
    }

    public Map<Integer, TLEPropagator> buildPropagators(List<Satellite> satellites) {
        long startMs = System.currentTimeMillis();
        Map<Integer, TLEPropagator> propagators = new HashMap<>();

        for (Satellite sat : satellites) {
            TLE tle = new TLE(sat.getTleLine1(), sat.getTleLine2());
            propagators.put(sat.getNoradCatId(), TLEPropagator.selectExtrapolator(tle));
        }

        log.debug("Built {} propagators in {}ms", propagators.size(), System.currentTimeMillis() - startMs);
        return propagators;
    }

    Map<Integer, PVCoordinates> propagateAll(Map<Integer, TLEPropagator> propagators, OffsetDateTime targetTime) {
        AbsoluteDate targetDate = toAbsoluteDate(targetTime);

        return propagators.entrySet().parallelStream()
                .<Map.Entry<Integer, PVCoordinates>>mapMulti((entry, consumer) -> {
                    try {
                        PVCoordinates pv = entry.getValue().getPVCoordinates(targetDate, entry.getValue().getFrame());
                        consumer.accept(Map.entry(entry.getKey(), pv));
                    } catch (Exception e) {
                        // Skip failed propagations during coarse scan
                    }
                })
                .collect(HashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        HashMap::putAll);
    }

    double calculateDistance(PVCoordinates pvA, PVCoordinates pvB) {
        double dx = (pvA.getPosition().getX() - pvB.getPosition().getX()) / 1000.0;
        double dy = (pvA.getPosition().getY() - pvB.getPosition().getY()) / 1000.0;
        double dz = (pvA.getPosition().getZ() - pvB.getPosition().getZ()) / 1000.0;
        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    double calculateRelativeVelocity(PVCoordinates pvA, PVCoordinates pvB) {
        double dvx = pvA.getVelocity().getX() - pvB.getVelocity().getX();
        double dvy = pvA.getVelocity().getY() - pvB.getVelocity().getY();
        double dvz = pvA.getVelocity().getZ() - pvB.getVelocity().getZ();
        return Math.sqrt(dvx * dvx + dvy * dvy + dvz * dvz);
    }

    AbsoluteDate toAbsoluteDate(OffsetDateTime dateTime) {
        return new AbsoluteDate(
                dateTime.getYear(),
                dateTime.getMonthValue(),
                dateTime.getDayOfMonth(),
                dateTime.getHour(),
                dateTime.getMinute(),
                dateTime.getSecond() + dateTime.getNano() / 1e9,
                TimeScalesFactory.getUTC()
        );
    }

    public record CoarseDetection(SatellitePair pair, OffsetDateTime time, double distance) {
    }
}
