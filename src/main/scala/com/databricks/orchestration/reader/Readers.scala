package com.databricks.orchestration.reader

import com.databricks.orchestration.Pipeline.PipelineNodeStatus
import com.databricks.orchestration.commons.OrchestrationConstant._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable


class SqlJdbcReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)

      val df = spark
        .read
        .format("jdbc")
        .options(this.readerOptions)
        .option("dbtable",this.tableName)
        .load()
        .limit(1000)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in jdbc reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class ParquetReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)

      val df = spark
        .read
        .format("parquet")
        .load(this.inputPath)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in parquet reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class JSONReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)
      val df = spark
        .read
        .format("json")
        .load(this.inputPath)
      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in json reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class ORCReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)

      val df = spark
        .read
        .format("orc")
        .load(this.inputPath)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in orc reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class AvroReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)

      val df = spark
        .read
        .format("avro")
        .load(this.inputPath)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in avro reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class CsvReader() extends BaseReader{
  override def call()={
    try {
//      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)
      println("hello")
      val df =

        if(this.readerOptions == null && !this.readerOptions.contains("schema"))
          spark.read.format("csv").load(this.inputPath)
        else if(!this.readerOptions.contains("schema"))
          spark.read.format("csv").options(this.readerOptions).load(this.inputPath)
        else
          spark.read.format("csv").options(this.readerOptions).schema(this.readerOptions.getOrElse("schema","")).load(this.inputPath)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in csv reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

  class SnowflakeReader() extends BaseReader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started, updateDB = true)
      var sfOptions = Map(
        "sfURL" -> sys.env.get("snowflake_url").getOrElse("").trim,
        "sfUser" -> sys.env.get("snowflake_username").getOrElse("").trim,
        "sfPassword" -> sys.env.get("snowflake_password").getOrElse("").trim,
        "sfDatabase" -> this.readerOptions.getOrElse("sfDbName","").trim,
        "sfSchema" -> this.readerOptions.getOrElse("sfSchemaName","PUBLIC").trim,
        "sfWarehouse" -> sys.env.get("snowflake_warehouse").getOrElse("").trim,
      )
      val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

      val df = this.readerOptions.get("sfQuery") match {
        case Some(query) => spark.read.format(SNOWFLAKE_SOURCE_NAME).options(sfOptions).option("query",  query.trim).load()
        case None => this.readerOptions.getOrElse("sfTableName", None) match {
          case Some(table) => spark.read.format(SNOWFLAKE_SOURCE_NAME).options(sfOptions).option("dbtable",  table.toString).load()
          case None => spark.emptyDataFrame
        }
      }

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in snowflake reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}
