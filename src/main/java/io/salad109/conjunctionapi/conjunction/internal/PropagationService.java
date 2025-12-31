package io.salad109.conjunctionapi.conjunction.internal;

import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatellitePair;
import org.orekit.propagation.analytical.tle.TLE;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.orekit.time.AbsoluteDate;
import org.orekit.time.TimeScalesFactory;
import org.orekit.utils.PVCoordinates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PropagationService {

    private static final Logger log = LoggerFactory.getLogger(PropagationService.class);

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

    /**
     * Propagate both satellites to a given time and return the distance between them.
     */
    public double propagateAndMeasureDistance(SatellitePair pair, Map<Integer, TLEPropagator> propagators, OffsetDateTime time) {
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
    public double propagateAndMeasureVelocity(SatellitePair pair, Map<Integer, TLEPropagator> propagators, OffsetDateTime time) {
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


    public Map<Integer, PVCoordinates> propagateAll(Map<Integer, TLEPropagator> propagators, OffsetDateTime targetTime) {
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

    public double calculateDistance(PVCoordinates pvA, PVCoordinates pvB) {
        double dx = (pvA.getPosition().getX() - pvB.getPosition().getX()) / 1000.0;
        double dy = (pvA.getPosition().getY() - pvB.getPosition().getY()) / 1000.0;
        double dz = (pvA.getPosition().getZ() - pvB.getPosition().getZ()) / 1000.0;
        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    public double calculateRelativeVelocity(PVCoordinates pvA, PVCoordinates pvB) {
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
}
