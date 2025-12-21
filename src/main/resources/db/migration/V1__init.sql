CREATE TABLE satellite
(
    norad_cat_id       INTEGER PRIMARY KEY,
    object_name        VARCHAR(25),
    object_type        VARCHAR(12),      -- PAYLOAD, ROCKET BODY, DEBRIS, UNKNOWN
    country_code       VARCHAR(6),
    launch_date        DATE,
    decay_date         DATE,

    -- Current TLE data
    epoch              TIMESTAMP WITH TIME ZONE,
    tle_line1          VARCHAR(71),
    tle_line2          VARCHAR(71),

    -- Orbital elements
    mean_motion        DOUBLE PRECISION, -- revs/day
    eccentricity       DOUBLE PRECISION, -- 0-1
    inclination        DOUBLE PRECISION, -- degrees
    raan               DOUBLE PRECISION, -- right ascension of ascending node, degrees
    arg_perigee        DOUBLE PRECISION, -- argument of perigee, degrees
    mean_anomaly       DOUBLE PRECISION, -- degrees
    bstar              DOUBLE PRECISION, -- drag term

    -- Derived values
    semi_major_axis_km DOUBLE PRECISION,
    perigee_km         DOUBLE PRECISION,
    apogee_km          DOUBLE PRECISION,
    orbital_period_min DOUBLE PRECISION
);

CREATE INDEX idx_satellite_orbital_shell ON satellite (perigee_km, apogee_km);
CREATE INDEX idx_satellite_inclination ON satellite (inclination);

-- Conjunctions table
CREATE TABLE conjunction
(
    id                     SERIAL PRIMARY KEY,
    object1_norad_id       INTEGER                  NOT NULL REFERENCES satellite (norad_cat_id),
    object2_norad_id       INTEGER                  NOT NULL REFERENCES satellite (norad_cat_id),
    miss_distance_km       DOUBLE PRECISION         NOT NULL,
    tca                    TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT objects_ordered CHECK (object1_norad_id < object2_norad_id)
);

CREATE INDEX idx_conjunction_tca ON conjunction (tca);
CREATE INDEX idx_conjunction_distance ON conjunction (miss_distance_km);

-- Ingestion log for tracking sync status
CREATE TABLE ingestion_log
(
    id               SERIAL PRIMARY KEY,
    started_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at     TIMESTAMP WITH TIME ZONE,
    objects_inserted INTEGER                  DEFAULT 0,
    objects_updated  INTEGER                  DEFAULT 0,
    objects_skipped  INTEGER                  DEFAULT 0,
    objects_deleted  INTEGER                  DEFAULT 0,
    successful       BOOLEAN                  DEFAULT FALSE,
    error_message    TEXT                     DEFAULT NULL
);

CREATE INDEX idx_ingestion_started_at ON ingestion_log (started_at);