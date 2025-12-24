package io.salad109.conjunctionapi.satellite;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SatelliteService {

    private final SatelliteRepository satelliteRepository;

    @Value("${ingestion.batch-size:1000}")
    private int batchSize;

    public SatelliteService(SatelliteRepository satelliteRepository) {
        this.satelliteRepository = satelliteRepository;
    }

    public Map<Integer, Satellite> getByCatalogIds(Iterable<Integer> catalogIds) {
        return satelliteRepository.findAllById(catalogIds).stream()
                .collect(Collectors.toMap(Satellite::getNoradCatId, Function.identity()));
    }

    @Transactional
    public int save(List<Satellite> satellites) {
        if (satellites.isEmpty()) {
            return 0;
        }

        for (int i = 0; i < satellites.size(); i += batchSize) {
            List<Satellite> batch = satellites.subList(i, Math.min(i + batchSize, satellites.size()));
            satelliteRepository.saveAll(batch);
            satelliteRepository.flush();
        }

        return satellites.size();
    }

    @Transactional
    public int deleteByCatalogIdsNotIn(Collection<Integer> catalogIds) {
        return satelliteRepository.deleteSatellitesByNoradCatIdNotIn(catalogIds);
    }
}
