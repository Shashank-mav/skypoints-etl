
-- Three-layer Medallion Architecture:
--   Bronze (Raw/Landing) → Silver (Staging) → Gold (Country + Redemptions)
 

-- -------------------------
-- 0. Create Database
-- -------------------------
CREATE DATABASE IF NOT EXISTS skypoints;
USE skypoints;

 
-- BRONZE LAYER — Raw / Landing
 


CREATE TABLE IF NOT EXISTS bronze_members (
    member_id       STRING,
    member_name     STRING,
    date_of_birth   STRING,
    tier_code       STRING,
    enrollment_date STRING,
    last_flight_date STRING,
    agent_name      STRING,
    state           STRING,
    country         STRING,
    post_code       STRING,
    is_active       STRING,
    individual_or_corporate STRING,

    -- Metadata 
    source_file     STRING      COMMENT 'Original filename (e.g. USA.csv)',
    source_country  STRING      COMMENT 'Country derived from filename',
    load_timestamp  TIMESTAMP   COMMENT 'When this record was ingested'
)
USING DELTA
COMMENT 'Bronze landing table — raw member data, all strings, no transformations'
TBLPROPERTIES ('quality' = 'bronze');


 
-- SILVER LAYER — Staging
 


CREATE TABLE IF NOT EXISTS silver_members (
    member_id       STRING      NOT NULL,
    member_name     STRING      NOT NULL,
    date_of_birth   DATE,
    tier_code       STRING,
    enrollment_date DATE        NOT NULL,
    last_flight_date DATE,
    agent_name      STRING,
    state           STRING,
    country         STRING      NOT NULL,
    post_code       STRING,
    is_active       STRING,
    individual_or_corporate STRING,

    -- Derived columns
    age             INT         COMMENT 'Computed from DOB',
    stale_member    BOOLEAN     COMMENT 'TRUE if days since last_flight_date > 90',

    -- Metadata / Lineage
    source_file     STRING,
    source_country  STRING,
    load_timestamp  TIMESTAMP,
    processed_timestamp TIMESTAMP COMMENT 'When this record was transformed'
)
USING DELTA
COMMENT 'Silver staging table — typed, standardized, with derived columns'
TBLPROPERTIES ('quality' = 'silver');


 
-- GOLD LAYER — Country-Specific Tables


CREATE TABLE IF NOT EXISTS gold_members_usa (
    member_id       STRING      NOT NULL,
    member_name     STRING      NOT NULL,
    date_of_birth   DATE,
    tier_code       STRING,
    enrollment_date DATE        NOT NULL,
    last_flight_date DATE,
    agent_name      STRING,
    state           STRING,
    country         STRING      NOT NULL,
    post_code       STRING,
    is_active       STRING,
    individual_or_corporate STRING,
    age             INT,
    stale_member    BOOLEAN,
    source_file     STRING,
    load_timestamp  TIMESTAMP,
    updated_timestamp TIMESTAMP COMMENT 'Last time this record was upserted'
)
USING DELTA
COMMENT 'Gold table — USA members'
TBLPROPERTIES ('quality' = 'gold');


CREATE TABLE IF NOT EXISTS gold_members_ind (
    member_id       STRING      NOT NULL,
    member_name     STRING      NOT NULL,
    date_of_birth   DATE,
    tier_code       STRING,
    enrollment_date DATE        NOT NULL,
    last_flight_date DATE,
    agent_name      STRING,
    state           STRING,
    country         STRING      NOT NULL,
    post_code       STRING,
    is_active       STRING,
    individual_or_corporate STRING,
    age             INT,
    stale_member    BOOLEAN,
    source_file     STRING,
    load_timestamp  TIMESTAMP,
    updated_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table — India members'
TBLPROPERTIES ('quality' = 'gold');


CREATE TABLE IF NOT EXISTS gold_members_aus (
    member_id       STRING      NOT NULL,
    member_name     STRING      NOT NULL,
    date_of_birth   DATE,
    tier_code       STRING,
    enrollment_date DATE        NOT NULL,
    last_flight_date DATE,
    agent_name      STRING,
    state           STRING,
    country         STRING      NOT NULL,
    post_code       STRING,
    is_active       STRING,
    individual_or_corporate STRING,
    age             INT,
    stale_member    BOOLEAN,
    source_file     STRING,
    load_timestamp  TIMESTAMP,
    updated_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table — Australia members'
TBLPROPERTIES ('quality' = 'gold');


 
-- GOLD LAYER — Redemption Transactions (flattened from JSON)
 

CREATE TABLE IF NOT EXISTS gold_redemptions (
    member_id       STRING      NOT NULL,
    feed_date       DATE        NOT NULL,
    txn_id          STRING      NOT NULL,
    txn_date        DATE,
    partner         STRING,
    miles_redeemed  BIGINT,
    status          STRING,

    -- Metadata
    source_file     STRING,
    load_timestamp  TIMESTAMP
)
USING DELTA
COMMENT 'Gold table — flattened redemption transactions from JSON feed'
TBLPROPERTIES ('quality' = 'gold');


 
-- QUARANTINE TABLE — Records that fail validation
 
-- Bad records land here 

CREATE TABLE IF NOT EXISTS quarantine_members (
    member_id       STRING,
    member_name     STRING,
    raw_record      STRING      COMMENT 'Full original record as string',
    validation_errors STRING    COMMENT 'list of failed checks',
    source_file     STRING,
    source_country  STRING,
    quarantined_at  TIMESTAMP
)
USING DELTA
COMMENT 'Records that failed data validation checks'
TBLPROPERTIES ('quality' = 'quarantine');
