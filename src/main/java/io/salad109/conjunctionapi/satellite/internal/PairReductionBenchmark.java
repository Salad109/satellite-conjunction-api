package io.salad109.conjunctionapi.satellite.internal;

import io.salad109.conjunctionapi.satellite.PairReductionService;
import io.salad109.conjunctionapi.satellite.Satellite;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

/**
 * Run on Linux: ./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-filter
 * Run on Windows: ./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-filter"
 */
@Component
@Profile("benchmark-filter")
public class PairReductionBenchmark implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PairReductionBenchmark.class);
    private static final double DEFAULT_TOLERANCE_KM = 50.0;

    private final SatelliteRepository satelliteRepository;
    private final PairReductionService pairReductionService;

    public PairReductionBenchmark(SatelliteRepository satelliteRepository, PairReductionService pairReductionService) {
        this.satelliteRepository = satelliteRepository;
        this.pairReductionService = pairReductionService;
    }

    @Override
    public void run(String @NonNull ... args) {
        log.info("Starting pair reduction benchmark");

        List<Satellite> satellites = satelliteRepository.findAll();
        int satelliteCount = satellites.size();
        long totalPairs = (long) satelliteCount * (satelliteCount - 1) / 2;

        log.info("Loaded {} satellites ({} total pairs)", satelliteCount, String.format("%,d", totalPairs));
        log.info("");

        BenchmarkResult noReduction = benchmarkStrategy(
                "no reduction",
                satellites,
                totalPairs,
                (a, b) -> true
        );

        BenchmarkResult noDebris = benchmarkStrategy(
                "no debris on debris",
                satellites,
                totalPairs,
                pairReductionService::neitherAreDebris
        );

        BenchmarkResult altitudesOverlap = benchmarkStrategy(
                "altitudes overlap",
                satellites,
                totalPairs,
                (a, b) -> pairReductionService.altitudeShellsOverlap(a, b, DEFAULT_TOLERANCE_KM)
        );

        BenchmarkResult planesIntersect = benchmarkStrategy(
                "planes intersect",
                satellites,
                totalPairs,
                (a, b) -> pairReductionService.orbitalPlanesIntersect(a, b, DEFAULT_TOLERANCE_KM)
        );

        BenchmarkResult allStrategies = benchmarkStrategy(
                "all strategies",
                satellites,
                totalPairs,
                (a, b) -> pairReductionService.canCollide(a, b, DEFAULT_TOLERANCE_KM)
        );

        printResultsTable(totalPairs, noReduction, noDebris, altitudesOverlap, planesIntersect, allStrategies);
    }

    private BenchmarkResult benchmarkStrategy(
            String name,
            List<Satellite> satellites,
            long totalPairs,
            BiPredicate<Satellite, Satellite> filter
    ) {
        log.info("Benchmarking: {}", name);
        int satelliteCount = satellites.size();

        LongAdder passingPairs = new LongAdder();

        long startTime = System.nanoTime();

        IntStream.range(0, satelliteCount).parallel().forEach(i -> {
            Satellite a = satellites.get(i);
            long localCount = 0;
            for (int j = i + 1; j < satelliteCount; j++) {
                Satellite b = satellites.get(j);
                if (filter.test(a, b)) {
                    localCount++;
                }
            }
            passingPairs.add(localCount);
        });

        long elapsedNanos = System.nanoTime() - startTime;
        long resultCount = passingPairs.sum();

        log.info(" -> {} pairs in {}ms", String.format("%,d", resultCount), elapsedNanos / 1_000_000);

        return new BenchmarkResult(name, resultCount, elapsedNanos, totalPairs);
    }

    private void printResultsTable(long totalPairs, BenchmarkResult... results) {
        log.info("");
        log.info("=".repeat(80));
        log.info("BENCHMARK RESULTS ({} total pairs)", String.format("%,d", totalPairs));
        log.info("=".repeat(80));
        log.info(String.format("%-19s | %12s | %13s | %10s | %14s",
                "strategy", "unique pairs", "% of full set", "time", "throughput/sec"));
        log.info("-".repeat(80));

        for (BenchmarkResult result : results) {
            double percentOfFull = 100.0 * result.passingPairs / result.totalPairs;
            double elapsedSeconds = result.elapsedNanos / 1_000_000_000.0;
            double throughput = result.totalPairs / elapsedSeconds;

            String timeStr = elapsedSeconds >= 1.0
                    ? String.format("%.2fs", elapsedSeconds)
                    : String.format("%.0fms", result.elapsedNanos / 1_000_000.0);

            log.info(String.format("%-19s | %12s | %12.2f%% | %10s | %14s",
                    result.name,
                    String.format("%,d", result.passingPairs),
                    percentOfFull,
                    timeStr,
                    String.format("%,.0f", throughput)));
        }

        log.info("=".repeat(80));
    }

    private record BenchmarkResult(String name, long passingPairs, long elapsedNanos, long totalPairs) {
    }
}