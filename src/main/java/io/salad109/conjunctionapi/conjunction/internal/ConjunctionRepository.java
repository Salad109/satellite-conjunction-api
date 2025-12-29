package io.salad109.conjunctionapi.conjunction.internal;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface ConjunctionRepository extends JpaRepository<Conjunction, Integer>, ConjunctionRepositoryCustom {

    @Query("SELECT new io.salad109.conjunctionapi.conjunction.internal.ConjunctionInfo(" +
            "c.missDistanceKm, c.tca, c.relativeVelocityMS, " +
            "c.object1NoradId, s1.objectName, s1.objectType, " +
            "c.object2NoradId, s2.objectName, s2.objectType) " +
            "FROM Conjunction c " +
            "JOIN Satellite s1 ON c.object1NoradId = s1.noradCatId " +
            "JOIN Satellite s2 ON c.object2NoradId = s2.noradCatId " +
            "WHERE c.relativeVelocityMS > 10")
    Page<ConjunctionInfo> getConjunctionInfos(Pageable pageable);


    @Query("SELECT new io.salad109.conjunctionapi.conjunction.internal.ConjunctionInfo(" +
            "c.missDistanceKm, c.tca, c.relativeVelocityMS, " +
            "c.object1NoradId, s1.objectName, s1.objectType, " +
            "c.object2NoradId, s2.objectName, s2.objectType) " +
            "FROM Conjunction c " +
            "JOIN Satellite s1 ON c.object1NoradId = s1.noradCatId " +
            "JOIN Satellite s2 ON c.object2NoradId = s2.noradCatId")
    Page<ConjunctionInfo> getConjunctionInfosWithFormations(Pageable pageable);
}
