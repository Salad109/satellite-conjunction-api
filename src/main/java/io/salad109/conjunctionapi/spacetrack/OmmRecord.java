package io.salad109.conjunctionapi.spacetrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * OMM (Orbit Mean-Elements Message) record from Space-Track GP class.
 * Field names match the Space-Track JSON response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OmmRecord(
        @JsonProperty("NORAD_CAT_ID") Integer noradCatId,
        @JsonProperty("OBJECT_NAME") String objectName,
        @JsonProperty("OBJECT_TYPE") String objectType,
        @JsonProperty("COUNTRY_CODE") String countryCode,
        @JsonProperty("LAUNCH_DATE") LocalDate launchDate,
        @JsonProperty("DECAY_DATE") LocalDate decayDate,
        @JsonProperty("EPOCH") LocalDateTime epoch,
        @JsonProperty("TLE_LINE1") String tleLine1,
        @JsonProperty("TLE_LINE2") String tleLine2,
        @JsonProperty("MEAN_MOTION") Double meanMotion,
        @JsonProperty("ECCENTRICITY") Double eccentricity,
        @JsonProperty("INCLINATION") Double inclination,
        @JsonProperty("RA_OF_ASC_NODE") Double raan,
        @JsonProperty("ARG_OF_PERICENTER") Double argPerigee,
        @JsonProperty("MEAN_ANOMALY") Double meanAnomaly,
        @JsonProperty("BSTAR") Double bstar,

        // Additional metadata
        @JsonProperty("ELEMENT_SET_NO") Integer elementSetNo,
        @JsonProperty("REV_AT_EPOCH") Integer revAtEpoch,
        @JsonProperty("SEMIMAJOR_AXIS") Double semiMajorAxis,
        @JsonProperty("PERIOD") Double period,
        @JsonProperty("APOAPSIS") Double apoapsis,
        @JsonProperty("PERIAPSIS") Double periapsis
) {
    /**
     * Validates that this record has the minimum required fields.
     */
    public boolean isValid() {
        return noradCatId != null
                && tleLine1 != null && !tleLine1.isBlank()
                && tleLine2 != null && !tleLine2.isBlank()
                && epoch != null
                && meanMotion != null && meanMotion > 0;
    }

    /**
     * Get the epoch as OffsetDateTime in UTC.
     */
    public OffsetDateTime getEpochUtc() {
        return epoch != null ? epoch.atOffset(ZoneOffset.UTC) : null;
    }
}
