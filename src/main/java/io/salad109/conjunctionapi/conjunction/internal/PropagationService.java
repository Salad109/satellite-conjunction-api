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
import java.util.stream.IntStream;

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


    /**
     * Pre-compute positions for all satellites across all time steps.
     */
    PositionCache precomputePositions(Map<Integer, TLEPropagator> propagators,
                                      OffsetDateTime startTime, int stepSeconds, int totalSteps,
                                      int interpolationStride) {
        int stride = Math.max(1, interpolationStride);
        log.debug("Pre-computing positions: {} sats, {} steps, stride={}", propagators.size(), totalSteps, stride);
        long startMs = System.currentTimeMillis();

        OffsetDateTime[] times = new OffsetDateTime[totalSteps];
        for (int i = 0; i < totalSteps; i++) {
            times[i] = startTime.plusSeconds((long) i * stepSeconds);
        }

        Integer[] satIds = propagators.keySet().toArray(Integer[]::new);
        Map<Integer, Integer> noradIdToArrayId = HashMap.newHashMap(satIds.length);
        for (int i = 0; i < satIds.length; i++) {
            noradIdToArrayId.put(satIds[i], i);
        }

        int numSats = satIds.length;
        double[][] x = new double[numSats][totalSteps];
        double[][] y = new double[numSats][totalSteps];
        double[][] z = new double[numSats][totalSteps];
        boolean[][] valid = new boolean[numSats][totalSteps];

        IntStream.range(0, numSats).parallel().forEach(s -> {
            TLEPropagator prop = propagators.get(satIds[s]);

            // SGP4 at stride points
            for (int step = 0; step < totalSteps; step += stride) {
                try {
                    PVCoordinates pv = prop.getPVCoordinates(toAbsoluteDate(times[step]), prop.getFrame());
                    x[s][step] = pv.getPosition().getX() / 1000.0;
                    y[s][step] = pv.getPosition().getY() / 1000.0;
                    z[s][step] = pv.getPosition().getZ() / 1000.0;
                    valid[s][step] = true;
                } catch (Exception e) {
                    valid[s][step] = false;
                }
            }

            // Linear interpolation between strides
            for (int a = 0; a + stride < totalSteps; a += stride) {
                int b = a + stride;
                if (!valid[s][a] || !valid[s][b]) continue;
                for (int step = a + 1; step < b; step++) {
                    double t = (double) (step - a) / stride;
                    x[s][step] = x[s][a] + t * (x[s][b] - x[s][a]);
                    y[s][step] = y[s][a] + t * (y[s][b] - y[s][a]);
                    z[s][step] = z[s][a] + t * (z[s][b] - z[s][a]);
                    valid[s][step] = true;
                }
            }
        });

        log.debug("Position pre-computation completed in {}ms", System.currentTimeMillis() - startMs);
        return new PositionCache(noradIdToArrayId, times, x, y, z, valid);
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

    record PositionCache(Map<Integer, Integer> noradIdToArrayId, OffsetDateTime[] times,
                         double[][] x, double[][] y, double[][] z,
                         boolean[][] valid) {
        public double distanceSquaredAt(int a, int b, int step, double tolSq) {
            double dx = x[a][step] - x[b][step];
            double dxSq = dx * dx;
            if (dxSq > tolSq) return dxSq;

            double dy = y[a][step] - y[b][step];
            double dySq = dxSq + dy * dy;
            if (dySq > tolSq) return dySq;

            double dz = z[a][step] - z[b][step];
            return dySq + dz * dz;
        }

        public boolean validAt(int a, int b, int step) {
            return valid[a][step] && valid[b][step];
        }
    }
}
