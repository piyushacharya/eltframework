//package com.databricks.orchestration.processor
//
//import com.databricks.orchestration.Pipeline.PipelineNodeStatus
//import com.databricks.orchestration.commons.OrchestrationConstant.{PROCESSEDDF, RAWDF}
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions.col
//
//import scala.collection.mutable
//
//class ColumnSelectorProcessor() extends BaseProcessor{
//
//  override def call()= {
//    try{
//    val rawDf = this.inputDataframe(PROCESSEDDF)
//    val targetCols = this.processorOptions.get("targetCols").get.split(",").map(x => x.trim)
//    val rawCols = rawDf.columns
//    val colMapping = this.processorOptions.get("colMapping").get.split(",").map(x=> x.trim.toInt).map(x => rawCols(x -1))
//
//    val colsToSelect = targetCols.zip(colMapping).map(x=> col(x._2).alias(x._1))
//    val returnMap=new mutable.HashMap[String,DataFrame]()
//    returnMap += PROCESSEDDF -> rawDf.select(colsToSelect:_*)
//    returnMap += RAWDF -> rawDf
//    taskOutputDataFrames = returnMap
//    updateAndStoreStatus(PipelineNodeStatus.Finished)
//    } catch {
//    case exception :Exception =>{
//      logger.error(s"got exception in ColumnSelector reader ${this.taskName}")
//      logger.error(exception.getMessage,exception)
//      updateAndStoreStatus(PipelineNodeStatus.Error,exception)
//    }
//  }
//  }
//}
//
//class SyncWithExistingTableProcessor() extends BaseProcessor{
//  override def call()= {
//    try{
//      val rawDf = this.inputDataframe(PROCESSEDDF)
//      val existingSchema = spark.read.table(this.databaseName + "." + this.tableName).schema
//      val colsToSelect = existingSchema.map(x => col(x.name).cast(x.dataType) )
//      val returnMap=new mutable.HashMap[String,DataFrame]()
//      returnMap += PROCESSEDDF -> rawDf.select(colsToSelect:_*)
//      returnMap += RAWDF -> rawDf
//      taskOutputDataFrames = returnMap
//      updateAndStoreStatus(PipelineNodeStatus.Finished)
//    } catch {
//      case exception :Exception =>{
//        logger.error(s"got exception in SyncWithExistingTableProcessor reader ${this.taskName}")
//        logger.error(exception.getMessage,exception)
//        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
//      }
//    }
//  }
//}
//
//
