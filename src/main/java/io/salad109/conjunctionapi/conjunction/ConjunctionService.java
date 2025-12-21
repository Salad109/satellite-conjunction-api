package io.salad109.conjunctionapi.conjunction;

import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteRepository;
import org.orekit.propagation.analytical.tle.TLE;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.orekit.time.AbsoluteDate;
import org.orekit.time.TimeScalesFactory;
import org.orekit.utils.PVCoordinates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Service
public class ConjunctionService {

    private static final Logger log = LoggerFactory.getLogger(ConjunctionService.class);

    private static final double TOLERANCE_KM = 10.0;
    private static final double CONJUNCTION_THRESHOLD_KM = 5.0;

    private final SatelliteRepository satelliteRepository;
    private final ConjunctionRepository conjunctionRepository;

    public ConjunctionService(SatelliteRepository satelliteRepository, ConjunctionRepository conjunctionRepository) {
        this.satelliteRepository = satelliteRepository;
        this.conjunctionRepository = conjunctionRepository;
    }

    @Transactional
    public List<Conjunction> findConjunctions() {
        long startMs = System.currentTimeMillis();
        log.info("Starting conjunction screening...");

        List<Satellite> satellites = satelliteRepository.findAll();
        List<SatellitePair> pairs = findPotentialCollisionPairs(satellites);
        Map<Integer, TLEPropagator> propagators = buildPropagators(satellites);

        OffsetDateTime targetTime = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(60);
        Map<Integer, PVCoordinates> positions = propagateAll(propagators, targetTime);
        List<Conjunction> conjunctions = findCloseApproaches(pairs, positions, targetTime);

        saveClosestApproaches(conjunctions);

        log.info("Conjunction screening completed in {}ms. Found {} conjunctions", System.currentTimeMillis() - startMs, conjunctions.size());
        return conjunctions;
    }

    private Map<Integer, TLEPropagator> buildPropagators(List<Satellite> satellites) {
        long startMs = System.currentTimeMillis();
        Map<Integer, TLEPropagator> propagators = new HashMap<>();
        int skipped = 0;

        for (Satellite sat : satellites) {
            try {
                if (sat.getEccentricity() != null && sat.getEccentricity() >= 1.0) {
                    skipped++;
                    continue;
                }
                TLE tle = new TLE(sat.getTleLine1(), sat.getTleLine2());
                propagators.put(sat.getNoradCatId(), TLEPropagator.selectExtrapolator(tle));
            } catch (Exception e) {
                skipped++;
            }
        }

        log.debug("Built {} propagators ({} skipped) in {}ms", propagators.size(), skipped, System.currentTimeMillis() - startMs);
        return propagators;
    }

    private Map<Integer, PVCoordinates> propagateAll(Map<Integer, TLEPropagator> propagators, OffsetDateTime targetTime) {
        long startMs = System.currentTimeMillis();
        AbsoluteDate targetDate = toAbsoluteDate(targetTime);
        AtomicInteger errors = new AtomicInteger();

        Map<Integer, PVCoordinates> positions = propagators.entrySet().parallelStream()
                .<Map.Entry<Integer, PVCoordinates>>mapMulti((entry, consumer) -> {
                    try {
                        PVCoordinates pv = entry.getValue().getPVCoordinates(targetDate, entry.getValue().getFrame());
                        consumer.accept(Map.entry(entry.getKey(), pv));
                    } catch (Exception e) {
                        errors.getAndIncrement();
                    }
                })
                .collect(HashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        HashMap::putAll);

        log.debug("Propagated {} satellites in {}ms ({} failed)", positions.size(), System.currentTimeMillis() - startMs, errors);
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
                    if (distance <= CONJUNCTION_THRESHOLD_KM) {
                        consumer.accept(new Conjunction(null, pair.a().getNoradCatId(), pair.b().getNoradCatId(), distance, time));
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

    private List<SatellitePair> findPotentialCollisionPairs(List<Satellite> satellites) {
        int satelliteCount = satellites.size();

        return IntStream.range(0, satelliteCount)
                .parallel()
                .boxed()
                .mapMulti((Integer i, Consumer<SatellitePair> consumer) -> {
                    Satellite a = satellites.get(i);
                    for (int j = i + 1; j < satelliteCount; j++) {
                        Satellite b = satellites.get(j);
                        if (PairReduction.canCollide(a, b, TOLERANCE_KM)) {
                            consumer.accept(new SatellitePair(a, b));
                        }
                    }
                })
                .toList();
    }

    public void saveClosestApproaches(List<Conjunction> conjunctions) {
        long startMs = System.currentTimeMillis();

        for (Conjunction c : conjunctions) {
            conjunctionRepository.upsertIfCloser(
                    c.getObject1NoradId(),
                    c.getObject2NoradId(),
                    c.getMissDistanceKm(),
                    c.getTca()
            );
        }

        log.debug("Upserted {} conjunctions in {}ms", conjunctions.size(), System.currentTimeMillis() - startMs);
    }

    private record SatellitePair(Satellite a, Satellite b) {
    }
}
