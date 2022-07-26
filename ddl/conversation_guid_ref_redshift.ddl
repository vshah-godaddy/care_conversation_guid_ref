use care_local;

DROP TABLE IF EXISTS conversation_guid_ref_redshift;

CREATE TABLE conversation_guid_ref_redshift
(
  parent_contact_id STRING
	,conversation_start_utc_ts  TIMESTAMP
	,conversation_end_utc_ts TIMESTAMP
	,conversation_end_utc_date TIMESTAMP
	,app_id STRING
	,conversation_source STRING
	,visitor_device_type STRING
	,visit_guid STRING
	,etl_build_mst_ts TIMESTAMP
	,rpt_mst_date STRING
)
STORED AS ORC
LOCATION
       's3://gd-ckpetlbatch-prod-analytic/care_local/conversation_guid_ref_redshift/'
;
