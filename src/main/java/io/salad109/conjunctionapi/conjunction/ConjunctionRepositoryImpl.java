package io.salad109.conjunctionapi.conjunction;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
public class ConjunctionRepositoryImpl implements ConjunctionRepositoryCustom {

    private final JdbcTemplate jdbcTemplate;

    public ConjunctionRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void batchUpsertIfCloser(List<Conjunction> conjunctions) {
        if (conjunctions.isEmpty()) {
            return;
        }

        String sql = """
                INSERT INTO conjunction (object1_norad_id, object2_norad_id, miss_distance_km, tca, relative_velocity_m_s)
                VALUES (?, ?, ?, ?, ?)
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
                    END,
                    relative_velocity_m_s = CASE
                        WHEN EXCLUDED.miss_distance_km < conjunction.miss_distance_km
                        THEN EXCLUDED.relative_velocity_m_s
                        ELSE conjunction.relative_velocity_m_s
                END
            """;

        jdbcTemplate.batchUpdate(sql, conjunctions, conjunctions.size(),
                (ps, conjunction) -> {
                    ps.setInt(1, conjunction.getObject1NoradId());
                    ps.setInt(2, conjunction.getObject2NoradId());
                    ps.setDouble(3, conjunction.getMissDistanceKm());
                    ps.setTimestamp(4, Timestamp.from(conjunction.getTca().toInstant()));
                    ps.setDouble(5, conjunction.getRelativeVelocityMS());
                });
    }
}