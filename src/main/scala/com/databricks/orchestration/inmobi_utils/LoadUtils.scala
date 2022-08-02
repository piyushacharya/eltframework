package com.databricks.orchestration.inmobi_utils

import com.databricks.orchestration.Pipeline.{Pipeline, PipelineBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import com.databricks.orchestration.reader._
import com.databricks.orchestration.processor._
import com.databricks.orchestration.writer.DeltaWriter
import com.databricks.orchestration.commons.OrchestrationConstant.{ERROR_TABLE_NAME, FACT_TABLE_NAME, ORCHESTRATION_DB,MOUNT_DETAILS_TABLE_NAME, STATUS_TABLE_NAME, ENTITY_TABLE_NAME, ENTITY_OPS_TABLE_NAME, ENTITY_STATS_TABLE_NAME}

import org.apache.spark.sql.types.StringType
import org.apache.log4j.Logger


object LoadUtils {
  private val logger = Logger.getLogger(getClass)

  def buildPipelineUsingOpId(spark:SparkSession, op_id: String):Pipeline = {
    import spark.implicits._
    val operations = spark.read.table(s"""${ORCHESTRATION_DB}.${ENTITY_OPS_TABLE_NAME}""").filter(s"op_id == '${op_id}'")
      .withColumn("op_details", $"details").drop("details")
    val entities = spark.read.table(s"""${ORCHESTRATION_DB}.${ENTITY_TABLE_NAME}""")
    val runDetails = entities.join(operations, entities("id") === operations("entity_id"), "inner").head
    pipelineBuilder(spark, runDetails)
  }

  def pipelineBuilder(spark:SparkSession, runDetails: Row): Pipeline ={
    spark.conf.set("spark.sql.legacy.charVarcharAsString","true")
    val mountPaths = spark.sql(s"select dbName, source_mount from ${ORCHESTRATION_DB}.${MOUNT_DETAILS_TABLE_NAME}").collect().map(x => (x.getAs[String](0),x.getAs[String](1))).toMap
    val outputMountPaths = spark.sql(s"select dbName, target_mount from ${ORCHESTRATION_DB}.${MOUNT_DETAILS_TABLE_NAME}").collect().map(x => (x.getAs[String](0),x.getAs[String](1))).toMap
    val database_name = runDetails.getAs[String]("db_name")
    val mountPath = mountPaths.getOrElse(database_name,"")
    val outputMountPath = outputMountPaths.getOrElse(database_name,"")
    val runId = runDetails.getAs[String]("op_id")
    val pipelineId = runDetails.getAs[String]("entity_id")
    val entity_name = runDetails.getAs[String]("name")
    val entityDetails = runDetails.getAs[Map[String,String]]("details")
    val delimiter = entityDetails.getOrElse("delimiter",",")
    val path = mountPath + runDetails.getAs[Map[String,String]]("op_details").getOrElse("path","")
    val partitionKeys = entityDetails.getOrElse("partition_column", "").toLowerCase
    val writerOptions = if(entityDetails.contains("partition_column") && runDetails.getAs[Map[String,String]]("op_details").contains("replaceWhere"))
      Map("partitionKeys" ->  entityDetails("partition_column").toLowerCase, "replaceWhere" -> runDetails.getAs[Map[String,String]]("op_details")("replaceWhere")) else if (entityDetails.contains("partition_column"))
      Map("partitionKeys" ->  entityDetails("partition_column").toLowerCase) else  Map[String,String]()
    logger.info("Writer Options - " + writerOptions)
    val processorOptions = entityDetails
    logger.info("Processor Options - " + processorOptions)
    val readerOptions = entityDetails("format") match {
      case "csv" => {
        if(entityDetails.contains("header") && entityDetails("header") == "true"){
          Map("delimiter" -> delimiter, "header" -> "true")
        }else{
          val colsInHeader = if(runDetails.getAs[Map[String,String]]("op_details").contains("headerPath"))
            spark.read.option("delimiter", "\u0001").csv(mountPath + runDetails.getAs[Map[String,String]]("op_details")("headerPath")).collect()(0).toSeq.map(x => x.toString)
          else
            entityDetails("headerCols").split("\\|").toSeq
          val tableCols = spark.sql(s"""DESCRIBE TABLE ${database_name}.${entity_name}""").collect().map(x => (x.getAs[String](0).toLowerCase,x.getAs[String](1))).toMap
          val schemaStr = colsInHeader.map(col => col + " " + tableCols.getOrElse(col.toLowerCase,"string")).mkString(",")
          println(s"schemaStr $schemaStr")
          Map("delimiter" -> delimiter, "schema" -> schemaStr)
        }
      }
      case "snowflake" => entityDetails.++(runDetails.getAs[Map[String,String]]("op_details"))
      case _ => null
    }
    println(s"readerOptions $readerOptions")
    val reader = entityDetails("format") match {
      case "parquet" => new ParquetReader()
      case "csv" => new CsvReader()
      case "snowflake" => new SnowflakeReader()
      case "orc" => new ORCReader()
      case _ => new ParquetReader()  // Default we choose parquet Reader
    }

    // Added to handle SSP, Programmatics fact tables partition updates (event_time|process_time)

    var opPath = s"$outputMountPath/data/$database_name/$entity_name"

    if (Set("ssp", "programmatics").contains(database_name)  && partitionKeys.startsWith("event_time")) {
      opPath = s"$outputMountPath/data/$database_name/load/$entity_name"
    }

    var pipelineBuilder = PipelineBuilder.start()
      .setPipelineName(s"${entity_name}").setRunId(runId).setPipelineDefId(pipelineId)
      .setInputPath(s"${path}")
      .setOutputPath(opPath)
      .setTableName(entity_name)
      .setDatabaseName(database_name)
      .setProductName("inmobi")
      .setReaderOptions(readerOptions)
      .setWriterOptions(writerOptions)
      .setProcessorOptions(processorOptions)
      .addSparkSession(spark)
      .addTask(s"reader-${runId}", reader)
    val pipeline = if(processorOptions.contains("targetCols") && processorOptions.contains("colMapping"))
      pipelineBuilder.addAfter(s"reader-${runId}",s"processor-${runId}", new ColumnSelectorProcessor()).addAfter(s"processor-${runId}",s"writer-${runId}", new DeltaWriter()).build()
    else
      pipelineBuilder.addAfter(s"reader-${runId}",s"processor-${runId}", new SyncWithExistingTableProcessor()).addAfter(s"processor-${runId}",s"writer-${runId}", new DeltaWriter()).build()
    pipeline
  }

  def createTableFromSnowflake(spark:SparkSession, sfDBName:String, sfSchemaName:String, sfTableName:String, dbName:String ,dbTableName:String, partitionCols:Seq[String] = Seq(), zindexCols:Seq[String] = Seq()){
    val outputMountPaths = spark.sql(s"select dbName, target_mount from ${ORCHESTRATION_DB}.${MOUNT_DETAILS_TABLE_NAME}").collect().map(x => (x.getAs[String](0),x.getAs[String](1))).toMap
    val zIndexColsLower = zindexCols.map(x => x.toLowerCase).toSeq
    val partitionColsLower = partitionCols.map(x => x.toLowerCase()).toSeq
    val sfOptions = Map(
      "sfURL" -> sys.env.get("snowflake_url").getOrElse("").trim,
      "sfUser" -> sys.env.get("snowflake_username").getOrElse("").trim,
      "sfPassword" -> sys.env.get("snowflake_password").getOrElse("").trim,
      "sfDatabase" -> sfDBName.trim,
      "sfSchema" -> sfSchemaName.trim,
      "sfWarehouse" -> sys.env.get("snowflake_warehouse").getOrElse("").trim,
    )
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    val df = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", s"SELECT * FROM ${sfTableName} limit 0")
      .load()
    val schema = df.schema
    val cols = schema.map(x => x.name.toLowerCase)
    val colsToSelect = zIndexColsLower ++ (cols diff zIndexColsLower)
    var opPath = outputMountPaths(dbName) + "/data/" + dbName + "/" + dbTableName

    if (Set("ssp", "programmatics").contains(dbName)) {
      opPath = outputMountPaths(dbName) + "/data/" + dbName + "/load/" + dbTableName
    }

    if(partitionColsLower.size == 0){
      df.selectExpr(colsToSelect:_*).write.format("delta").option("path", opPath ).saveAsTable(s"${dbName}.${dbTableName}")
    } else {
      df.selectExpr(colsToSelect:_*).write.format("delta").option("path", opPath ).partitionBy(partitionColsLower:_*).saveAsTable(s"${dbName}.${dbTableName}")
    }
  }
}
