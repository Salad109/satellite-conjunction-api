package io.salad109.conjunctionapi.conjunction;

import io.salad109.conjunctionapi.conjunction.internal.*;
import io.salad109.conjunctionapi.satellite.PairReductionService;
import io.salad109.conjunctionapi.satellite.Satellite;
import io.salad109.conjunctionapi.satellite.SatellitePair;
import io.salad109.conjunctionapi.satellite.SatelliteService;
import org.orekit.propagation.analytical.tle.TLEPropagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
public class ConjunctionService {

    private static final Logger log = LoggerFactory.getLogger(ConjunctionService.class);

    private final SatelliteService satelliteService;
    private final ConjunctionRepository conjunctionRepository;
    private final PairReductionService pairReductionService;
    private final PropagationService propagationService;
    private final ScanService scanService;

    @Value("${conjunction.tolerance-km:50.0}")
    private double toleranceKm;

    @Value("${conjunction.collision-threshold-km:5.0}")
    private double thresholdKm;

    @Value("${conjunction.lookahead-hours:24}")
    private int lookaheadHours;

    @Value("${conjunction.step-seconds:3}")
    private int stepSeconds;

    public ConjunctionService(SatelliteService satelliteService,
                              ConjunctionRepository conjunctionRepository,
                              PairReductionService pairReductionService,
                              PropagationService propagationService,
                              ScanService scanService) {
        this.satelliteService = satelliteService;
        this.conjunctionRepository = conjunctionRepository;
        this.pairReductionService = pairReductionService;
        this.propagationService = propagationService;
        this.scanService = scanService;
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

        // Load satellites
        List<Satellite> satellites = satelliteService.getAll();
        log.debug("Loaded {} satellites", satellites.size());

        // Find and filter potential collision pairs
        List<SatellitePair> pairs = pairReductionService.findPotentialCollisionPairs(satellites);
        log.debug("Reduced to {} candidate pairs", pairs.size());

        // Build propagators
        Map<Integer, TLEPropagator> propagators = propagationService.buildPropagators(satellites);

        // Scan for conjunctions
        List<Conjunction> conjunctions = scanService.scanForConjunctions(pairs, propagators, toleranceKm, thresholdKm, lookaheadHours, stepSeconds);

        // Save all conjunctions (upsert keeps closest per pair)
        if (!conjunctions.isEmpty()) {
            conjunctionRepository.batchUpsertIfCloser(conjunctions);
        }

        log.info("Conjunction screening completed in {}ms, found {} conjunctions",
                System.currentTimeMillis() - startMs, conjunctions.size());
    }
}
