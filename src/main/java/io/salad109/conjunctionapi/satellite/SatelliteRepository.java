package io.salad109.conjunctionapi.satellite;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SatelliteRepository extends JpaRepository<Satellite, Integer> {

    /**
     * Count satellites in catalog.
     */
    long count();
}
