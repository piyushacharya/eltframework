CREATE DATABASE orchestration_db;

DROP TABLE IF EXISTS orchestration_db.mount_details;
CREATE TABLE orchestration_db.mount_details  (dbName String, source_mount String, target_mount String)
USING DELTA LOCATION '<base_path>/orchestration_db/mount_details';

DROP TABLE IF EXISTS orchestration_db.Entity;
CREATE EXTERNAL TABLE orchestration_db.Entity
(id STRING,
db_name STRING,
name STRING,
entity_type STRING,
insert_dt TIMESTAMP,
latest_status STRING,
update_date STRING,
updated_at TIMESTAMP,
details MAP<STRING, STRING>)
USING DELTA LOCATION '<base_path/orchestration_db/entity';

DROP TABLE IF EXISTS orchestration_db.Entity_Operation_Details;
CREATE EXTERNAL TABLE orchestration_db.Entity_Operation_Details
(
op_id STRING,
entity_id STRING,
start_tm TIMESTAMP,
end_tm TIMESTAMP,
status STRING,
exception_details STRING,
updated_at timestamp,
details MAP<STRING, STRING>
) USING DELTA '<base_path>/orchestration_db/entity_operation_details';

DROP  TABLE IF EXISTS orchestration_db.Entity_Operation_Stats;
CREATE EXTERNAL TABLE orchestration_db.Entity_Operation_Stats
(
stat_id STRING,
stat_name STRING,
stat_value STRING,
updated_at timestamp
) USING DELTA '<base_path/orchestration_db/entity_operation_stats';