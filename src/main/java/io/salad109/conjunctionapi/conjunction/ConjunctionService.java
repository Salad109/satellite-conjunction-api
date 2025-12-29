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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class ConjunctionService {

    private static final Logger log = LoggerFactory.getLogger(ConjunctionService.class);
    private final SatelliteService satelliteService;
    private final ConjunctionRepository conjunctionRepository;
    private final PairReductionService pairReductionService;
    @Value("${conjunction.collision-threshold-km:5.0}")
    private double conjunctionThresholdKm;
    @Value("${conjunction.lookahead-hours:1}")
    private int lookaheadHours;
    @Value("${conjunction.step-seconds:60}")
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

        List<Satellite> satellites = satelliteService.getAll();
        log.debug("Loaded {} satellites from database in {}ms", satellites.size(), System.currentTimeMillis() - startMs);
        List<SatellitePair> pairs = pairReductionService.findPotentialCollisionPairs(satellites);
        Map<Integer, TLEPropagator> propagators = buildPropagators(satellites);
        OffsetDateTime startTime = OffsetDateTime.now(ZoneOffset.UTC);

        for (int offsetSeconds = 0; offsetSeconds <= lookaheadHours * 3600; offsetSeconds += stepSeconds) {
            OffsetDateTime targetTime = startTime.plusSeconds(offsetSeconds);
            Map<Integer, PVCoordinates> positions = propagateAll(propagators, targetTime);
            List<Conjunction> stepConjunctions = findCloseApproaches(pairs, positions, targetTime);
            saveClosestApproaches(stepConjunctions);
        }

        log.info("Conjunction screening completed in {}ms", System.currentTimeMillis() - startMs);
    }

    private Map<Integer, TLEPropagator> buildPropagators(List<Satellite> satellites) {
        long startMs = System.currentTimeMillis();
        Map<Integer, TLEPropagator> propagators = new HashMap<>();

        for (Satellite sat : satellites) {
            TLE tle = new TLE(sat.getTleLine1(), sat.getTleLine2());
            propagators.put(sat.getNoradCatId(), TLEPropagator.selectExtrapolator(tle));
        }

        log.debug("Built {} propagators in {}ms", propagators.size(), System.currentTimeMillis() - startMs);
        return propagators;
    }

    private Map<Integer, PVCoordinates> propagateAll(Map<Integer, TLEPropagator> propagators, OffsetDateTime targetTime) {
        long startMs = System.currentTimeMillis();
        AbsoluteDate targetDate = toAbsoluteDate(targetTime);

        Map<Integer, PVCoordinates> positions = propagators.entrySet().parallelStream()
                .<Map.Entry<Integer, PVCoordinates>>mapMulti((entry, consumer) -> {
                    try {
                        PVCoordinates pv = entry.getValue().getPVCoordinates(targetDate, entry.getValue().getFrame());
                        consumer.accept(Map.entry(entry.getKey(), pv));
                    } catch (Exception e) {
                        log.error("Failed to propagate satellite {}: {}", entry.getKey(), e.getMessage());
                    }
                })
                .collect(HashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        HashMap::putAll);

        log.debug("Propagated {} satellites in {}ms", positions.size(), System.currentTimeMillis() - startMs);
        return positions;
    }

    private List<Conjunction> findCloseApproaches(List<SatellitePair> pairs, Map<Integer, PVCoordinates> positions, OffsetDateTime time) {
        long startMs = System.currentTimeMillis();

        List<Conjunction> conjunctions = pairs.parallelStream()
                .mapMulti((SatellitePair pair, Consumer<Conjunction> consumer) -> {
                    PVCoordinates pvA = positions.get(pair.a().getNoradCatId());
                    PVCoordinates pvB = positions.get(pair.b().getNoradCatId());

                    if (pvA == null || pvB == null) return;

                    double distance = calculateDistance(pvA, pvB);
                    if (distance <= conjunctionThresholdKm) {
                        double relativeVelocityMS = calculateRelativeVelocity(pvA, pvB);
                        consumer.accept(new Conjunction(null, pair.a().getNoradCatId(), pair.b().getNoradCatId(), distance, time, relativeVelocityMS));
                    }
                })
                .toList();

        log.debug("Checked {} pairs in {}ms, found {} conjunctions", pairs.size(), System.currentTimeMillis() - startMs, conjunctions.size());
        return conjunctions;
    }

    private double calculateDistance(PVCoordinates pvA, PVCoordinates pvB) {
        double dx = (pvA.getPosition().getX() - pvB.getPosition().getX()) / 1000.0;
        double dy = (pvA.getPosition().getY() - pvB.getPosition().getY()) / 1000.0;
        double dz = (pvA.getPosition().getZ() - pvB.getPosition().getZ()) / 1000.0;
        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    private double calculateRelativeVelocity(PVCoordinates pvA, PVCoordinates pvB) {
        double dvx = pvA.getVelocity().getX() - pvB.getVelocity().getX();
        double dvy = pvA.getVelocity().getY() - pvB.getVelocity().getY();
        double dvz = pvA.getVelocity().getZ() - pvB.getVelocity().getZ();
        return Math.sqrt(dvx * dvx + dvy * dvy + dvz * dvz);
    }

    private AbsoluteDate toAbsoluteDate(OffsetDateTime dateTime) {
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

    private void saveClosestApproaches(List<Conjunction> conjunctions) {
        long startMs = System.currentTimeMillis();

        conjunctionRepository.batchUpsertIfCloser(conjunctions);

        log.debug("Upserted {} conjunctions in {}ms", conjunctions.size(), System.currentTimeMillis() - startMs);
    }
}
