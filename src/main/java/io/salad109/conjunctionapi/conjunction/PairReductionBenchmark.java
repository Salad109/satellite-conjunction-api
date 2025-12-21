package io.salad109.conjunctionapi.conjunction;

import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatelliteRepository;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Run on Linux: ./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-filter
 * Run on Windows: ./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-filter"
 */
@Component
@Profile("benchmark-filter")
public class PairReductionBenchmark implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PairReductionBenchmark.class);
    private static final double TOLERANCE_KM = 10.0;

    private final SatelliteRepository satelliteRepository;

    public PairReductionBenchmark(SatelliteRepository satelliteRepository) {
        this.satelliteRepository = satelliteRepository;
    }

    @Override
    public void run(String @NonNull ... args) {
        log.info("Starting pair reduction benchmark");

        List<Satellite> satellites = satelliteRepository.findAll();
        int satelliteCount = satellites.size();
        long totalPairs = (long) satelliteCount * (satelliteCount - 1) / 2;

        log.info("Loaded {} satellites", satelliteCount);

        AtomicLong passAllFilters = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        IntStream.range(0, satelliteCount).parallel().forEach(i -> {
            Satellite a = satellites.get(i);
            for (int j = i + 1; j < satelliteCount; j++) {
                Satellite b = satellites.get(j);
                if (PairReduction.canCollide(a, b, TOLERANCE_KM)) {
                    passAllFilters.incrementAndGet();
                }
            }
        });

        long elapsed = System.currentTimeMillis() - startTime;

        log.info("");
        log.info("Benchmark results:");
        log.info("Total pairs: {}",
                String.format("%,d", totalPairs));
        log.info("Remaining candidates: {} ({}%)",
                String.format("%,d", passAllFilters.get()),
                String.format("%.2f", 100.0 * passAllFilters.get() / totalPairs));
        log.info("Total reduction: {}%",
                String.format("%.2f", 100.0 * (1 - (double) passAllFilters.get() / totalPairs)));
        log.info("Elapsed time: {} seconds",
                String.format("%.2f", elapsed / 1000.0));
        log.info("Throughput: {} pairs/second",
                String.format("%,.0f", (double) totalPairs / (elapsed / 1000.0)));
    }
}
