package io.salad109.conjunctionapi.conjunction.internal;

import io.salad109.conjunctionapi.satellite.SatellitePair;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ScanService {

    private static final Logger log = LoggerFactory.getLogger(ScanService.class);

    private final PropagationService propagationService;

    public ScanService(PropagationService propagationService) {
        this.propagationService = propagationService;
    }

    public List<Conjunction> scanForConjunctions(List<SatellitePair> pairs, Map<Integer, TLEPropagator> propagators, double toleranceKm, double thresholdKm, int lookaheadHours, int stepSeconds, int interpolationStride) {
        log.debug("Starting conjunction scan for {} pairs over {} hours (tolerance={} km, threshold={} km, interpStride={})",
                pairs.size(), lookaheadHours, toleranceKm, thresholdKm, interpolationStride);
        // Coarse sweep
        List<CoarseDetection> coarseDetections = coarseSweep(pairs, propagators, OffsetDateTime.now(ZoneOffset.UTC), toleranceKm, stepSeconds, lookaheadHours, interpolationStride);
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
                                      OffsetDateTime startTime, double toleranceKm, int stepSeconds, int lookaheadHours,
                                      int interpolationStride) {
        long startMs = System.currentTimeMillis();

        int totalSteps = (lookaheadHours * 3600) / stepSeconds + 1;
        log.debug("Coarse sweep: {} steps over {} hours at {}s intervals", totalSteps, lookaheadHours, stepSeconds);

        // Pre-compute all satellite positions (with optional interpolation)
        PropagationService.PositionCache precomputedPositions = propagationService.precomputePositions(
                propagators, startTime, stepSeconds, totalSteps, interpolationStride);

        // Check all pairs
        List<CoarseDetection> detections = checkPairs(pairs, precomputedPositions, toleranceKm);

        log.debug("Coarse sweep completed in {}ms with {} total detections",
                System.currentTimeMillis() - startMs, detections.size());
        return detections;
    }

    private List<CoarseDetection> checkPairs(List<SatellitePair> pairs, PropagationService.PositionCache precomputedPositions,
                                             double toleranceKm) {
        log.debug("Checking {} pairs for close approaches", pairs.size());
        long checkStart = System.currentTimeMillis();

        double tolSq = toleranceKm * toleranceKm; // skip sqrt by comparing squared distances
        int totalSteps = precomputedPositions.times().length;

        List<CoarseDetection> detections = pairs.parallelStream()
                .<CoarseDetection>mapMulti((pair, consumer) -> {
                    Integer idxA = precomputedPositions.noradIdToArrayId().get(pair.a().getNoradCatId());
                    Integer idxB = precomputedPositions.noradIdToArrayId().get(pair.b().getNoradCatId());
                    if (idxA == null || idxB == null) return;

                    for (int step = 0; step < totalSteps; step++) {
                        if (!precomputedPositions.validAt(idxA, idxB, step)) continue;
                        double distSq = precomputedPositions.distanceSquaredAt(idxA, idxB, step);
                        if (distSq < tolSq) {
                            consumer.accept(new CoarseDetection(pair, precomputedPositions.times()[step], Math.sqrt(distSq)));
                        }
                    }
                })
                .toList();

        log.debug("Pair checking completed in {}ms", System.currentTimeMillis() - checkStart);
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

        Map<SatellitePair, List<List<CoarseDetection>>> result = byPair.entrySet().parallelStream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            List<CoarseDetection> sorted = entry.getValue().stream()
                                    .sorted(Comparator.comparing(CoarseDetection::time))
                                    .toList();
                            return clusterByTimeGap(sorted, gapThresholdSeconds);
                        }
                ));

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
     * Refine an event (cluster of coarse detections) using Brent's method to find more accurate TCA and minimum distance.
     */
    Conjunction refineEvent(List<CoarseDetection> event, Map<Integer, TLEPropagator> propagators, int stepSeconds) {
        CoarseDetection best = event.stream()
                .min(Comparator.comparing(CoarseDetection::distance))
                .orElseThrow();
        SatellitePair pair = best.pair();

        // Search interval is stepSeconds/2 on each side of best detection
        long halfWindowNanos = (stepSeconds * 1_000_000_000L) / 2;
        OffsetDateTime baseTime = best.time().minusNanos(halfWindowNanos);

        // Use Brent's method from Apache Commons Math
        // Only absolute tolerance matters. 50ms = 50,000,000 nanoseconds
        BrentOptimizer optimizer = new BrentOptimizer(1e-8, 50_000_000);

        UnivariateObjectiveFunction objectiveFunction = new UnivariateObjectiveFunction(
                offsetNanos -> propagationService.propagateAndMeasureDistance(pair, propagators, baseTime.plusNanos((long) offsetNanos))
        );

        UnivariatePointValuePair result = optimizer.optimize(
                objectiveFunction,
                GoalType.MINIMIZE,
                new SearchInterval(0, 2.0 * halfWindowNanos),
                MaxEval.unlimited()
        );

        OffsetDateTime tca = baseTime.plusNanos((long) result.getPoint());
        double minDistance = result.getValue();

        double relativeVelocity = propagationService.propagateAndMeasureVelocity(pair, propagators, tca);

        // Ensure object 1 norad id < object 2 norad id
        int object1 = Math.min(pair.a().getNoradCatId(), pair.b().getNoradCatId());
        int object2 = Math.max(pair.a().getNoradCatId(), pair.b().getNoradCatId());

        return new Conjunction(
                null,
                object1,
                object2,
                minDistance,
                tca,
                relativeVelocity
        );
    }

    record CoarseDetection(SatellitePair pair, OffsetDateTime time, double distance) {
    }
}
