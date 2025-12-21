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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public List<Conjunction> findConjunctions() {
        long startMs = System.currentTimeMillis();
        log.info("Starting conjunction screening (single 60s propagation)...");

        List<Satellite> satellites = satelliteRepository.findAll();
        log.info("Loaded {} satellites from catalog", satellites.size());

        List<SatellitePair> potentialCollisionPairs = findPotentialCollisionPairs(satellites);
        log.info("Found {} potential collision pairs after filtering", potentialCollisionPairs.size());

        // Build propagators for all satellites
        log.info("Building propagators...");
        Map<Integer, TLEPropagator> propagators = new HashMap<>();
        int skippedInvalid = 0;
        for (Satellite sat : satellites) {
            try {
                if (sat.getEccentricity() != null && sat.getEccentricity() >= 1.0) {
                    skippedInvalid++;
                    continue;
                }

                TLE tle = new TLE(sat.getTleLine1(), sat.getTleLine2());
                propagators.put(sat.getNoradCatId(), TLEPropagator.selectExtrapolator(tle));
            } catch (Exception e) {
                skippedInvalid++;
            }
        }
        log.info("Built {} propagators ({} skipped)", propagators.size(), skippedInvalid);

        // Propagate 60 seconds into the future
        OffsetDateTime targetTime = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(60);
        AbsoluteDate targetDate = toAbsoluteDate(targetTime);

        log.info("Checking {} pairs...", potentialCollisionPairs.size());

        List<Conjunction> conjunctions = potentialCollisionPairs.parallelStream()
                .mapMulti((SatellitePair pair, Consumer<Conjunction> consumer) -> {
                    TLEPropagator propA = propagators.get(pair.a().getNoradCatId());
                    TLEPropagator propB = propagators.get(pair.b().getNoradCatId());

                    try {
                        PVCoordinates pvA = propagate(propA, targetDate);
                        PVCoordinates pvB = propagate(propB, targetDate);

                        double distance = calculateDistance(pvA, pvB);

                        if (distance <= CONJUNCTION_THRESHOLD_KM) {
                            consumer.accept(new Conjunction(
                                    null,
                                    pair.a().getNoradCatId(),
                                    pair.b().getNoradCatId(),
                                    distance,
                                    targetTime
                            ));
                        }
                    } catch (RuntimeException e) {
                        // Skip pairs that fail to propagate
                    }
                })
                .toList();

        long totalDurationMs = System.currentTimeMillis() - startMs;

        log.info("Checked {} pairs in {}ms, found {} conjunctions within {}km",
                potentialCollisionPairs.size(), totalDurationMs, conjunctions.size(), CONJUNCTION_THRESHOLD_KM);

        conjunctionRepository.saveAll(conjunctions);

        return conjunctions;
    }

    private PVCoordinates propagate(TLEPropagator propagator, AbsoluteDate date) {
        return propagator.getPVCoordinates(date, propagator.getFrame());
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

    private record SatellitePair(Satellite a, Satellite b) {
    }
}
