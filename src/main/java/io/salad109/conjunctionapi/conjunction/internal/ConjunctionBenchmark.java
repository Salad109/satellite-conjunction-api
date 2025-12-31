package io.salad109.conjunctionapi.conjunction.internal;

import io.salad109.conjunctionapi.satellite.PairReductionService;
import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatellitePair;
import io.salad109.conjunctionapi.satellite.SatelliteService;
import org.jspecify.annotations.NonNull;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Run on Linux: ./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-conjunction
 * Run on Windows: ./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-conjunction"
 */
@Component
@Profile("benchmark-conjunction")
public class ConjunctionBenchmark implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ConjunctionBenchmark.class);

    private final SatelliteService satelliteService;
    private final PairReductionService pairReductionService;
    private final PropagationService propagationService;
    private final ScanService scanService;

    public ConjunctionBenchmark(SatelliteService satelliteService, PairReductionService pairReductionService, PropagationService propagationService, ScanService scanService) {
        this.satelliteService = satelliteService;
        this.pairReductionService = pairReductionService;
        this.propagationService = propagationService;
        this.scanService = scanService;
    }

    @Override
    public void run(String @NonNull ... args) {
        log.info("Starting conjunction detection benchmark");
        log.info("");

        List<Satellite> satellites = satelliteService.getAll();
        log.info("Loaded {} satellites", satellites.size());

        // Use fixed start time midnight UTC today
        OffsetDateTime fixedStartTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        log.info("Using fixed start time: {}", fixedStartTime);

        List<SatellitePair> pairs = pairReductionService.findPotentialCollisionPairs(satellites);
        log.info("Reduced to {} candidate pairs", pairs.size());

        Map<Integer, TLEPropagator> propagators = propagationService.buildPropagators(satellites);
        log.info("Built {} propagators", propagators.size());
        log.info("");

        int lookaheadHours = 6;
        double thresholdKm = 5.0;

        List<BenchmarkResult> results = new ArrayList<>();

        // Run benchmarks with varying tolerance (60-600km, step 15) and step seconds (4-40s, step 1)
        double toleranceKm = 60;
        int stepSeconds = 4;
        while (toleranceKm <= 600) {
            results.add(runBenchmark(pairs, propagators, fixedStartTime, toleranceKm, stepSeconds, lookaheadHours, thresholdKm));
            toleranceKm += 15;
            stepSeconds += 1;
        }

        printResultsTable(results);
        writeCsvResults(results);
    }

    private BenchmarkResult runBenchmark(
            List<SatellitePair> allPairs,
            Map<Integer, TLEPropagator> propagators,
            OffsetDateTime startTime,
            double toleranceKm,
            int stepSeconds,
            int lookaheadHours,
            double thresholdKm
    ) {
        String name = String.format("tol=%.0f, step=%d", toleranceKm, stepSeconds);
        log.info("Running: {}", name);
        long benchmarkStart = System.currentTimeMillis();

        long coarseStart = System.currentTimeMillis();
        List<ScanService.CoarseDetection> detections = scanService.coarseSweep(allPairs, propagators, startTime, toleranceKm, stepSeconds, lookaheadHours);
        long coarseTime = System.currentTimeMillis() - coarseStart;

        Map<SatellitePair, List<List<ScanService.CoarseDetection>>> eventsByPair = scanService.groupIntoEvents(detections, stepSeconds);
        int totalEvents = eventsByPair.values().stream().mapToInt(List::size).sum();

        List<List<ScanService.CoarseDetection>> allEvents = eventsByPair.values().stream()
                .flatMap(List::stream)
                .toList();

        long refineStart = System.currentTimeMillis();
        List<Conjunction> refined = allEvents.parallelStream().map(event -> scanService.refineEvent(event, propagators, stepSeconds))
                .filter(c -> c.getMissDistanceKm() <= thresholdKm)
                .toList();
        long refineTime = System.currentTimeMillis() - refineStart;

        List<Conjunction> deduplicated = refined.stream()
                .collect(Collectors.toMap(
                        c -> c.getObject1NoradId() + ":" + c.getObject2NoradId(),
                        c -> c,
                        (a, b) -> a.getMissDistanceKm() <= b.getMissDistanceKm() ? a : b
                ))
                .values()
                .stream()
                .toList();

        long totalTime = System.currentTimeMillis() - benchmarkStart;

        log.info(" -> {} detections, {} events, {} conjunctions, {} dedup in {}ms",
                detections.size(), totalEvents, refined.size(), deduplicated.size(), totalTime);

        return new BenchmarkResult(name, toleranceKm, stepSeconds, detections.size(), totalEvents,
                refined.size(), deduplicated.size(), coarseTime, refineTime, totalTime);
    }


    private void printResultsTable(List<BenchmarkResult> results) {
        log.info("");
        log.info("=".repeat(114));
        log.info("CONJUNCTION BENCHMARK RESULTS");
        log.info("=".repeat(114));
        log.info(String.format("%-18s | %8s | %6s | %10s | %8s | %6s | %6s | %8s | %8s | %8s",
                "config", "tol(km)", "step(s)", "detections", "events", "conj", "dedup", "coarse", "refine", "total"));
        log.info("-".repeat(114));

        for (BenchmarkResult r : results) {
            log.info(String.format("%-18s | %8.0f | %7d | %10s | %8s | %6d | %6d | %7.1fs | %7.1fs | %7.1fs",
                    r.name, r.toleranceKm, r.stepSeconds,
                    formatCount(r.detections), formatCount(r.events),
                    r.conjunctions, r.deduplicated,
                    r.coarseTimeMs / 1000.0, r.refineTimeMs / 1000.0, r.totalTimeMs / 1000.0));
        }

        log.info("=".repeat(114));
    }

    private String formatCount(long count) {
        if (count >= 1_000_000) return String.format("%.1fM", count / 1_000_000.0);
        if (count >= 1_000) return String.format("%.0fK", count / 1_000.0);
        return String.valueOf(count);
    }

    private void writeCsvResults(List<BenchmarkResult> results) {
        Path outputPath = Paths.get("docs", "conjunction-plots", "conjunction_benchmark.csv");

        try {
            Files.createDirectories(outputPath.getParent());

            try (FileWriter writer = new FileWriter(outputPath.toFile())) {
                writer.write("tolerance_km,step_s,detections,events,conj,dedup,coarse_s,refine_s,total_s\n");

                for (BenchmarkResult r : results) {
                    writer.write(String.format("%.0f,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f%n",
                            r.toleranceKm,
                            r.stepSeconds,
                            r.detections,
                            r.events,
                            r.conjunctions,
                            r.deduplicated,
                            r.coarseTimeMs / 1000.0,
                            r.refineTimeMs / 1000.0,
                            r.totalTimeMs / 1000.0));
                }
            }

            log.info("");
            log.info("CSV results written to: {}", outputPath.toAbsolutePath());

        } catch (IOException e) {
            log.error("Failed to write CSV file", e);
        }
    }

    private record BenchmarkResult(String name, double toleranceKm, int stepSeconds, long detections,
                                   int events, int conjunctions, int deduplicated,
                                   long coarseTimeMs, long refineTimeMs, long totalTimeMs) {
    }
}
