package io.salad109.conjunctionapi.conjunction;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

@Entity
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "conjunction")
public class Conjunction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "object1_norad_id")
    private Integer object1NoradId;

    @Column(name = "object2_norad_id")
    private Integer object2NoradId;

    @Column(name = "miss_distance_km")
    private double missDistanceKm;

    @Column(name = "tca")
    private OffsetDateTime tca;

    @Column(name = "relative_velocity_m_s")
    private double relativeVelocityMS;
}
