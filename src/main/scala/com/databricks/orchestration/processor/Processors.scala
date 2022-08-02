package com.databricks.orchestration.processor

import com.databricks.orchestration.Pipeline.PipelineNodeStatus
import com.databricks.orchestration.commons.OrchestrationConstant.{PROCESSEDDF, RAWDF}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.StringType
import scala.collection.mutable

class ColumnSelectorProcessor() extends BaseProcessor {

  override def call() = {
    try {
      val rawDf = this.inputDataframe(PROCESSEDDF)
      val targetCols = this.processorOptions.get("targetCols").get.split(",").map(x => x.trim)
      val rawCols = rawDf.columns
      val colMapping = this.processorOptions.get("colMapping").get.split(",").map(x => x.trim.toInt).map(x => rawCols(x - 1))

      val colsToSelect = targetCols.zip(colMapping).map(x => col(x._2).alias(x._1))
      val returnMap = new mutable.HashMap[String, DataFrame]()
      returnMap += PROCESSEDDF -> rawDf.select(colsToSelect: _*)
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception: Exception => {
        logger.error(s"Got exception in ColumnSelector reader ${this.taskName}")
        logger.error(exception.getMessage, exception)
        updateAndStoreStatus(PipelineNodeStatus.Error, exception)
      }
    }
  }
}

class SyncWithExistingTableProcessor() extends BaseProcessor {
  override def call() = {
    try {
      var rawDf = this.inputDataframe(PROCESSEDDF)
      val existingSchema = spark.read.table(this.databaseName + "." + this.tableName).schema
      val colToDataTypeMapping = existingSchema.map(x => (x.name.toLowerCase, x.dataType)).toMap
      val colsToSelect = existingSchema.map(x => col(x.name).cast(x.dataType))
      println(s"Entity Details - ${this.processorOptions}")
      this.processorOptions.get("add_columns") match {
        case Some(value) => {
          logger.info(s"Processing for Adding Columns - ${value}")
          println(s"Processing for Adding Columns - ${value}")
          value.split("\\|").map(x => x.split("=")).foreach { x => {
            if (x.size != 2) throw new Exception("Properly configure the add_columns")
            if(!(rawDf.schema.map(x => x.name.toLowerCase).contains(x(0).toLowerCase))) {
              val value = if (x(1) == "null") lit(null) else expr(x(1))
              val colSearch = if (x(0).contains("_parsed")) x(0).replace("_parsed", "") else x(0)
              rawDf = rawDf.withColumn(x(0).toLowerCase, value.cast(colToDataTypeMapping.getOrElse(colSearch, StringType)))
            } else{
              println("Skipped adding the column - " + x(0))
            }
          }
          }
        }
        case None => logger.info("No columns need to be added")
      }
      this.processorOptions.get("rename_columns") match {
        case Some(value) => {
          logger.info(s"Processing for Rename Columns - ${value}")
          println(s"Processing for Rename Columns - ${value}")
          value.split("\\|").map(x => x.split("=")).foreach { x => {
            if (x.size != 2) throw new Exception("Properly configure the rename_columns")
            // rawDf = rawDf.withColumnRenamed(x(1), x(0))
            rawDf = rawDf.withColumn(x(0), expr(x(1))).drop(x(1))
          }
          }
        }
        case None => logger.info("No columns need to be renamed")
      }
      this.processorOptions.get("drop_columns") match {
        case Some(value) => {
          logger.info(s"Processing for Drop Columns - ${value}")
          value.split("\\|").map(x => x.toLowerCase()).foreach(x => rawDf = rawDf.drop(x))
        }
        case None => logger.info("No columns need to be dropped")
      }
      // println(s"Schema for raw df after processing - ${rawDf.schema}")
      val returnMap = new mutable.HashMap[String, DataFrame]()
      returnMap += PROCESSEDDF -> rawDf.select(colsToSelect: _*)
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception: Exception => {
        logger.error(s"got exception in SyncWithExistingTableProcessor reader ${this.taskName}")
        logger.error(exception.getMessage, exception)
        updateAndStoreStatus(PipelineNodeStatus.Error, exception)
      }
    }
  }
}


