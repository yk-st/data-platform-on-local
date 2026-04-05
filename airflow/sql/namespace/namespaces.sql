
CREATE NAMESPACE IF NOT EXISTS slv_entities
LOCATION 's3a://local-data-platform/warehouse/slv/slv_entities.db'
;


CREATE NAMESPACE IF NOT EXISTS gld_mart
LOCATION 's3a://local-data-platform/warehouse/gld/gld_mart.db'
;


CREATE NAMESPACE IF NOT EXISTS brz_ingestion
LOCATION 's3a://local-data-platform/warehouse/brz/brz_ingestion.db'
;


CREATE NAMESPACE IF NOT EXISTS ref
LOCATION 's3a://local-data-platform/warehouse/ref/ref.db'
;


CREATE NAMESPACE IF NOT EXISTS slv_fund
LOCATION 's3a://local-data-platform/warehouse/slv/slv_fund.db'
;

CREATE NAMESPACE IF NOT EXISTS gld_fund
LOCATION 's3a://local-data-platform/warehouse/gld/gld_fund.db'
;

create database IF NOT EXISTS spark_catalog.legacy;