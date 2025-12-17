package io.salad109.conjunctionapi.satellite;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Objects;

@Setter
@Getter
@Entity
@Table(name = "satellite")
public class Satellite {

    // Earth radius for TLE calculations (WGS72)
    private static final double EARTH_RADIUS_KM = 6378.135;
    private static final double MU = 398600.4418; // Earth gravitational parameter km^3/s^2

    @Id
    @Column(name = "norad_cat_id")
    private Integer noradCatId;

    @Column(name = "object_name")
    private String objectName;

    @Column(name = "object_type")
    private String objectType;

    @Column(name = "country_code")
    private String countryCode;

    @Column(name = "launch_date")
    private LocalDate launchDate;

    @Column(name = "decay_date")
    private LocalDate decayDate;

    // TLE data
    @Column(name = "epoch")
    private OffsetDateTime epoch;

    @Column(name = "tle_line1")
    private String tleLine1;

    @Column(name = "tle_line2")
    private String tleLine2;

    // Orbital elements
    @Column(name = "mean_motion")
    private Double meanMotion;

    @Column(name = "eccentricity")
    private Double eccentricity;

    @Column(name = "inclination")
    private Double inclination;

    @Column(name = "raan")
    private Double raan;

    @Column(name = "arg_perigee")
    private Double argPerigee;

    @Column(name = "mean_anomaly")
    private Double meanAnomaly;

    @Column(name = "bstar")
    private Double bstar;

    // Derived values for filtering
    @Column(name = "semi_major_axis_km")
    private Double semiMajorAxisKm;

    @Column(name = "perigee_km")
    private Double perigeeKm;

    @Column(name = "apogee_km")
    private Double apogeeKm;

    @Column(name = "orbital_period_min")
    private Double orbitalPeriodMin;

    public Satellite() {
    }

    public Satellite(Integer noradCatId) {
        this.noradCatId = noradCatId;
    }

    @PrePersist
    @PreUpdate
    protected void recalculate() {
        computeDerivedParameters();
    }

    /**
     * Compute derived orbital parameters from mean motion and eccentricity.
     * Call this before persisting.
     */
    public void computeDerivedParameters() {
        if (meanMotion == null || meanMotion <= 0 || eccentricity == null) {
            return;
        }

        // Convert mean motion from rev/day to rad/s
        double n = meanMotion * 2 * Math.PI / 86400.0;

        // Semi-major axis from Kepler's third law: a = (mu/n^2)^(1/3)
        this.semiMajorAxisKm = Math.pow(MU / (n * n), 1.0 / 3.0);

        // Perigee and apogee altitudes
        this.perigeeKm = semiMajorAxisKm * (1 - eccentricity) - EARTH_RADIUS_KM;
        this.apogeeKm = semiMajorAxisKm * (1 + eccentricity) - EARTH_RADIUS_KM;

        // Orbital period in minutes
        this.orbitalPeriodMin = 1440.0 / meanMotion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Satellite satellite)) return false;
        return Objects.equals(noradCatId, satellite.noradCatId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(noradCatId);
    }
}
