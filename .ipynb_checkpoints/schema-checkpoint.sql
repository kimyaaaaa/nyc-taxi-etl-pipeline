-- ======================================
-- DROP (for rebuilds during development)
-- ======================================

DROP TABLE IF EXISTS trips_fact CASCADE;
DROP TABLE IF EXISTS datetime_dim CASCADE;
DROP TABLE IF EXISTS location_dim CASCADE;
DROP TABLE IF EXISTS vendor_dim CASCADE;
DROP TABLE IF EXISTS payment_dim CASCADE;
DROP TABLE IF EXISTS ratecode_dim CASCADE;

-- ======================================
-- DIMENSIONS
-- ======================================
CREATE TABLE IF NOT EXISTS etl_metadata (
    year INTEGER,
    month INTEGER,
    loaded_at TIMESTAMP,
    PRIMARY KEY (year, month)
);

CREATE TABLE vendor_dim (
    vendor_id SMALLINT PRIMARY KEY,
    vendor_name TEXT
);

CREATE TABLE payment_dim (
    payment_type_id SMALLINT PRIMARY KEY,
    payment_description TEXT
);

CREATE TABLE ratecode_dim (
    ratecode_id SMALLINT PRIMARY KEY,
    rate_description TEXT
);

-- ================
-- SCD TYPE 2
-- ================
CREATE TABLE location_dim (
    location_sk BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL,

    borough TEXT,
    zone TEXT,
    service_zone TEXT,

    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_location_nk 
ON location_dim(location_id);

CREATE INDEX idx_location_current 
ON location_dim(is_current);

-- =================
-- DATETIME DIM
-- =================
CREATE TABLE datetime_dim (
    datetime_id BIGSERIAL PRIMARY KEY,
    full_datetime TIMESTAMP UNIQUE,

    minute SMALLINT,
    hour SMALLINT,
    day SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT,
    weekday_name TEXT,
    is_weekend BOOLEAN,
    time_of_day TEXT
);

CREATE INDEX idx_datetime_full 
ON datetime_dim(full_datetime);

-- ======================================
-- FACT TABLE
-- ======================================

CREATE TABLE trips_fact (
    trip_id BIGSERIAL PRIMARY KEY,

    pickup_datetime_id BIGINT 
        REFERENCES datetime_dim(datetime_id),

    dropoff_datetime_id BIGINT 
        REFERENCES datetime_dim(datetime_id),

    pickup_location_sk BIGINT 
        REFERENCES location_dim(location_sk),

    dropoff_location_sk BIGINT 
        REFERENCES location_dim(location_sk),

    payment_type_id SMALLINT 
        REFERENCES payment_dim(payment_type_id),

    vendor_id SMALLINT 
        REFERENCES vendor_dim(vendor_id),

    ratecode_id SMALLINT 
        REFERENCES ratecode_dim(ratecode_id),

    passenger_count SMALLINT,
    trip_distance NUMERIC(6,2),

    trip_duration_minutes NUMERIC(6,2),
    avg_speed_mph NUMERIC(6,2),

    fare_amount NUMERIC(8,2),
    extra NUMERIC(8,2),
    mta_tax NUMERIC(8,2),
    tip_amount NUMERIC(8,2),
    tolls_amount NUMERIC(8,2),
    improvement_surcharge NUMERIC(8,2),
    congestion_surcharge NUMERIC(8,2),
    airport_fee NUMERIC(8,2),
    cbd_congestion_fee NUMERIC(8,2),
    total_amount NUMERIC(8,2),

    is_tipped BOOLEAN,
    store_and_fwd_flag CHAR(1)
);

-- ======================
-- FACT INDEXES
-- ======================

CREATE INDEX idx_fact_pickup_dt 
ON trips_fact(pickup_datetime_id);

CREATE INDEX idx_fact_dropoff_dt 
ON trips_fact(dropoff_datetime_id);

CREATE INDEX idx_fact_pickup_loc 
ON trips_fact(pickup_location_sk);

CREATE INDEX idx_fact_dropoff_loc
ON trips_fact(dropoff_location_sk);

CREATE INDEX idx_fact_vendor 
ON trips_fact(vendor_id);

CREATE INDEX idx_fact_payment 
ON trips_fact(payment_type_id);
