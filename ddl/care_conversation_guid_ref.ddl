CREATE TABLE IF NOT EXISTS care_local.conversation_guid_ref
(
    conversation_id STRING,
    conversation_start_utc_ts  TIMESTAMP,
    conversation_end_utc_ts  TIMESTAMP,
    visit_guid  STRING,
    conversation_end_utc_date  TIMESTAMP,
    app_id STRING,
    conversation_source STRING,
    visitor_device_type STRING,
    etl_build_mst_ts TIMESTAMP,
    rpt_mst_date STRING
)PARTITIONED BY (rpt_mst_date DATE)
STORED AS PARQUET LOCATION 's3://gd-ckpetlbatch-${environment}-analytic/care_local/care_conversation_guid_ref';
