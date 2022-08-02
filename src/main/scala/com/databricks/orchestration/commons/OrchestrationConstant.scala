package com.databricks.orchestration.commons

object OrchestrationConstant {

  val SQLITE_URL = "jdbc:sqlite:/Users/sriram.mohanty/sqlite3-test/test.db"
 val AVRO_SCHEMA_REG_URL = "http://test.in:8081/"

  val MATCH = "match"
  val NOTMATCH = "notmatch"
  val DELETE ="delete"
  val READER = "_reader"
  val WRITER = "_writer"
  val PROCESSOR = "_processer"
  val CDC_READER = "_cdc_reader"
  val CDC_WRITER = "_cdc_writer"
  val CDC_PROCESSOR = "_cdc_processer"
  val HISTORY_READER = "_history_reader"
  val HISTORY_WRITER = "_history_writer"
  val HISTORY_PROCESSOR = "_history_processer"
  val JDBC_READER = "_jdbc_reader"
  val JDBC_WRITER = "_jdbc_writer"
  val JDBC_PROCESSOR = "_jdbc_processor"
  val HISTORY_TABLE_TAG = "_history"


  val NUMBITS224 = 224
  val NUMBITS256 = 256
  val NUMBITS384 = 384
  val NUMBITS512 = 512
  val NUMBITS0 = 0 //equivalent to 256

  val ORCHESTRATION_DB = "orchestration_db"
  val PII_COLUMN_DETAILS_TABLE_NAME = "pii_column_details"
  val TABLE_TOPIC_DETAILS_TABLE_NAME = "table_details"
  val SHARD_DETAILS_TABLE_NAME = "shard_details"
  val INTERNAL_SHARD_MAPPINGS_TABLE_NAME = "internal_shard_mappings"
  val PIPELINE_BATCH_MAP = "pipeline_batch_map"
  val FACT_TABLE_NAME = "pipeline_fact"
  val STATUS_TABLE_NAME = "pipeline_status"
  val ERROR_TABLE_NAME = "pipeline_error_logs"
  val RAWDF = "rawdf"
  val PROCESSEDDF = "processedDf"
  val READER_TYPE="reader_type"
  val READER_OPTIONS="reader_options"
 val ENTITY_TABLE_NAME = "entity"
 val ENTITY_OPS_TABLE_NAME = "Entity_Operation_Details"
 val ENTITY_STATS_TABLE_NAME = "Entity_Operation_Stats"
 val MOUNT_DETAILS_TABLE_NAME = "mount_details"
}

object StatusColumns extends Enumeration {
 val input, partition, startOffset, endOffset, pipelineName, instanceName, status,pipelineId,runId,errorMsg,stackTrace,lastUpdate,batchId,pipelineDefId,lastUpdateDate = Value
}

object EntityColumns extends Enumeration {
 val id, db_name, name, entity_type, insert_dt, latest_status, update_date, updated_at, details  = Value
}

object EntityOperationDetailsColumns extends Enumeration {
 val op_id, entity_id, start_tm, end_tm, status, exception_details, details, updated_at = Value
}

object EntityOperationStatsColumns extends Enumeration {
 val stat_id, stat_name, stat_value, updated_at = Value
}

object FactColumns extends Enumeration {
 val runId, pipelineId, pipelineName, instanceName, input, inputCount, outputCount,lastUpdate,pipelineDefId,lastUpdateDate = Value
}

object ErrorTableColumns extends Enumeration {
 val errorData, insertTime = Value
}

object PipeLineType extends Enumeration {
 val CDC, JDBC,HTTP = Value
}