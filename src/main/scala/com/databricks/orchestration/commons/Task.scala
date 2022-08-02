package com.databricks.orchestration.commons

import com.databricks.orchestration.Pipeline.PipelineNodeStatus
import com.databricks.orchestration.commons.OrchestrationConstant.{ENTITY_OPS_TABLE_NAME, ENTITY_STATS_TABLE_NAME, ENTITY_TABLE_NAME, ERROR_TABLE_NAME, FACT_TABLE_NAME, ORCHESTRATION_DB, STATUS_TABLE_NAME}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, max}

import java.util.concurrent.Callable
import scala.collection.mutable
import io.delta.exceptions.DeltaConcurrentModificationException
//import com.inmobi.rna.partition.RegisterLensPartitions

object TaskOutputType extends Enumeration {
  val None, dataframe, temp_table = Value
}

/**
 * Task Class is the core entity of the Orchestration framework.Each Reader/Processor/Writer are task internally.
 */
abstract class Task extends Callable[Any] {
  var spark: SparkSession = null
  var taskName: String = null
  var taskOutputType: TaskOutputType.Value = TaskOutputType.None
  var taskOutputTableName: String = null
  var taskOutputDataFrames: mutable.Map[String, DataFrame] = null
  var inputDataframe: mutable.Map[String, DataFrame] = null
  var pipelineName: String = null
  var pipeLineUid: String = null
  var runId: String = null
  var batchId: String = null
  var taskStatus: PipelineNodeStatus.Value = null;
  var taskFacts: List[Tuple3[String, String, String]] = null;
  var productName: String = null;
  var topicName: String = null;
  var logger: Logger = null;
  var tableName: String = null
  var databaseName: String = null
  var partitionSize: Int = 0
  var pipelineDefId: String = null
  var readerOptions: Map[String, String] = null
  var writerOptions: Map[String, String] = null
  var processorOptions: Map[String, String] = null
  var inputPath: String = null
  var outputPath: String = null
  var updateStatus = false


  def updateRetryableExceptionStatus(status: PipelineNodeStatus.Value, exception: Exception = null): Unit = {
    taskStatus = status
    var flag = true
    val startTm = EntityOperationDetailsColumns.start_tm
    val endTm = "CURRENT_TIMESTAMP"
    val exceptionStr = if (exception != null)  ExceptionUtils.getStackTrace(exception) else null
    val operations = spark.read.table(s"""${ORCHESTRATION_DB}.${ENTITY_OPS_TABLE_NAME}""").filter(s"op_id == '${runId}'").head
    var entityOpDetails = operations.getAs[Map[String,String]]("details")
    var retryCount: Int = entityOpDetails.getOrElse("retry_count", "0").toInt

    if (retryCount == 3) taskStatus = PipelineNodeStatus.Error else retryCount += 1

    entityOpDetails = entityOpDetails.+(("retry_count", retryCount.toString))
    val detailsMap = entityOpDetails.map(tpl => s"'${tpl._1}' , '${tpl._2}'").mkString(", ")
    println(s"detailsMap $detailsMap")

    val entityOpsUpdateSql =
      s"""UPDATE $ORCHESTRATION_DB.$ENTITY_OPS_TABLE_NAME
         | SET ${EntityOperationDetailsColumns.status} = '$taskStatus'
         | , ${EntityOperationDetailsColumns.start_tm} = $startTm
         | , ${EntityOperationDetailsColumns.end_tm} = $endTm
         |  , ${EntityOperationDetailsColumns.details} = map($detailsMap)
         | , updated_at = CURRENT_TIMESTAMP
         | , exception_details = "$exceptionStr"
         | WHERE ${EntityOperationDetailsColumns.op_id} == '${runId}'""".stripMargin

    println(s"entityOpsUpdateSql $entityOpsUpdateSql")

    while (flag) {
      try {
        spark.sql(entityOpsUpdateSql)
        flag = false
      } catch {
        case exception: DeltaConcurrentModificationException => {
          logger.warn("Got Concurrent Modification Exception. Rerunning")
        }
        case exception: Exception => {
          throw exception
        }
      }
    }
  }
  def updateAndStoreStatus(status: PipelineNodeStatus.Value, exception: Exception = null, updateDB : Boolean = false): Any = {

    if (updateStatus == false)
      taskStatus = status;
      return None;

    try {
      taskStatus = status;
      if (status == PipelineNodeStatus.Error ||  updateDB){
        val entityUpdateSql =
          s"""UPDATE ${ORCHESTRATION_DB}.${ENTITY_TABLE_NAME}
             | SET ${EntityColumns.latest_status} = '${taskStatus}'
             | , updated_at = CURRENT_TIMESTAMP
             | WHERE ${EntityColumns.id} == '${pipelineDefId}'""".stripMargin
        var flag = true
        while (flag) {
          try {
            spark.sql(entityUpdateSql)
            flag = false
          } catch {
            case exception: DeltaConcurrentModificationException => {
              logger.warn("Got Concurrent Modification Exception. Rerunning")
            }
            case exception: Exception => {
              throw exception
            }
          }
        }
        val startTm = if (taskStatus == PipelineNodeStatus.Started) "CURRENT_TIMESTAMP" else EntityOperationDetailsColumns.start_tm
        val endTm = if (Seq(PipelineNodeStatus.Error, PipelineNodeStatus.Finished).contains(taskStatus)) "CURRENT_TIMESTAMP" else EntityOperationDetailsColumns.end_tm
        val exceptionStr = if (exception != null)  ExceptionUtils.getStackTrace(exception) else null

        val entityOpsUpdateSql =
          s"""UPDATE ${ORCHESTRATION_DB}.${ENTITY_OPS_TABLE_NAME}
             | SET ${EntityOperationDetailsColumns.status} = '${taskStatus}'
             | , ${EntityOperationDetailsColumns.start_tm} = ${startTm}
             | , ${EntityOperationDetailsColumns.end_tm} = ${endTm}
             | , updated_at = CURRENT_TIMESTAMP
             | , exception_details = "$exceptionStr"
             | WHERE ${EntityOperationDetailsColumns.op_id} == '${runId}'""".stripMargin
        flag = true
        while (flag) {
          try {
            spark.sql(entityOpsUpdateSql)
            flag = false
          } catch {
            case exception: DeltaConcurrentModificationException => {
              logger.warn("Got Concurrent Modification Exception. Rerunning")
            }
            case exception: Exception => {
              throw exception
            }
          }
        }
      }
    } catch {
      case exception: Exception => {
        exception.printStackTrace()
        logger.error(exception.getMessage, exception)
      }
    }
  }

  def registerLensPartitions(tableName: String, databaseName: String, processTime: String): Unit = {
    val mountLocation = "/dbfs/mnt/ssp/deltabatchstore/reporting/modules/partition_registration_lib/"
    val args =
      Array[String]("-partition_registration_path", mountLocation + databaseName + "/" + tableName + "/partition_registration.properties",
        "-unified_facts_path", mountLocation + databaseName + "/" + tableName + "/unified_facts.properties",
        "-processingDate" , processTime)
    logger.info(s"LENS Registration for processTime: ${processTime}, db name: ${databaseName} & table: ${tableName} for processTime: ${processTime}")
    println(s"LENS Registration for processTime: ${processTime}, db name: ${databaseName} & table: ${tableName} for processTime: ${processTime}")
//    RegisterLensPartitions.main(args);
  }

  def storeOperationStats() {
    val spark_l = this.spark
    import spark_l.implicits._
    try {
      val stats = getMetrics(s"""${this.databaseName}.${this.tableName}""").first.getAs[Map[String, String]]("operationMetrics")
      val stat_df = stats.toSeq.toDF(
        EntityOperationStatsColumns.stat_name.toString, EntityOperationStatsColumns.stat_value.toString
      ).withColumn(
        EntityOperationStatsColumns.stat_id.toString, lit(this.runId)).withColumn(
        EntityOperationStatsColumns.updated_at.toString, current_timestamp())
      var flag = true
      while (flag) {
        try {
          stat_df.write.format("delta").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${ENTITY_STATS_TABLE_NAME}""")
          flag = false
        } catch {
          case exception: DeltaConcurrentModificationException => {
            logger.warn("Got Concurrent Modification Exception. Rerunning")
          }
          case exception: Exception => {
            throw exception
          }
        }
      }
    } catch {
      case exception: Exception => {
        exception.printStackTrace()
        logger.error(exception.getMessage, exception)
      }
    }
  }

  def getMetrics(tableName: String): DataFrame = {
    spark.sql(s"describe history $tableName limit 1").selectExpr("operationMetrics")
  }

}