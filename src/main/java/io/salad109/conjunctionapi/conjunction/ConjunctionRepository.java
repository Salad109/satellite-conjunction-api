package io.salad109.conjunctionapi.conjunction;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.Optional;

public interface ConjunctionRepository extends JpaRepository<Conjunction, Integer> {
    Optional<Conjunction> findByObject1NoradIdAndObject2NoradId(Integer object1NoradId, Integer object2NoradId);

    @Modifying
    @Query(value = """
            INSERT INTO conjunction (object1_norad_id, object2_norad_id, miss_distance_km, tca)
            VALUES (:obj1, :obj2, :distance, :tca)
            ON CONFLICT (object1_norad_id, object2_norad_id)
            DO UPDATE SET
                miss_distance_km = CASE
                    WHEN EXCLUDED.miss_distance_km < conjunction.miss_distance_km
                    THEN EXCLUDED.miss_distance_km
                    ELSE conjunction.miss_distance_km
                END,
                tca = CASE
                    WHEN EXCLUDED.miss_distance_km < conjunction.miss_distance_km
                    THEN EXCLUDED.tca
                    ELSE conjunction.tca
                END
            """, nativeQuery = true)
    void upsertIfCloser(@Param("obj1") Integer obj1,
                        @Param("obj2") Integer obj2,
                        @Param("distance") Double distance,
                        @Param("tca") OffsetDateTime tca);
}
