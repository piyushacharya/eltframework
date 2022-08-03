package com.databricks.orchestration.writer

import com.databricks.orchestration.Pipeline.PipelineNodeStatus
import com.databricks.orchestration.commons.OrchestrationConstant._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import com.databricks.orchestration.utility.Utility.checkTable
import io.delta.exceptions.DeltaConcurrentModificationException
import io.delta.tables.DeltaTable
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp

class DeltaWriterInMobi extends BaseWriter {
  override def call(): Any = {
    var flag = true
    var retryCount: Int = 0
    while (flag) {
      try {
        updateAndStoreStatus(PipelineNodeStatus.Started)
        logger.info("INFO: Delta writer started")
        var rawDf = this.inputDataframe(PROCESSEDDF)
        if (this.tableName.equalsIgnoreCase("PRICING_DAILY_FACT")) {
          rawDf = rawDf.filter("year(process_time) <= year(current_date()) AND " +
            "year(request_time) <= year(current_date()) " + "AND year(process_time) >= 1990 " +
            " AND year(request_time) >= 1990")
        }
        val tableName = s"""${this.databaseName}.${this.tableName}"""
        val partitionKeys = this.writerOptions.get("partitionKeys")
        val partitionValues = this.writerOptions.get("partitionValues")
        val replaceWhere = this.writerOptions.get("replaceWhere")
        val writeMode = this.writerOptions.get("mode") match {
          case Some(value) => value.toLowerCase match {
            case "append" => "append"
            case _ => "overwrite"
          }
          case None => "overwrite"
        }
        partitionKeys match {
          case Some(partition) => {
            val partitions = partition.split("\\|").toSeq

            val processTimePartn = partitions.filter(_.contains("process_time"))

            if (processTimePartn.nonEmpty && Set("ssp", "programmatics").contains(databaseName)) {
              val tsEncoder = rawDf.sparkSession.implicits.newTimeStampEncoder
              val deletePartns = rawDf.selectExpr(processTimePartn: _*).distinct.map(_.getAs[Timestamp](0))(tsEncoder)
                .collect().map(tsValue => s"process_time = '$tsValue' ").mkString(" OR ")
              val deleteQuery = s" DELETE FROM $tableName WHERE $deletePartns"
              logger.info(s"INFO: deletePartns $deletePartns ")
              println(s"PRINT DELETE PARTNS: deletePartns $deletePartns & $deleteQuery")

              val del = rawDf.sparkSession.sql(deleteQuery)
              del.show(false)
            }

            val condition = ListBuffer[String]()
            partitionValues match {
              case Some(values) => {
                val partVal = values.split("\\|").toSeq
                partitions.zip(partVal).foreach { x => {
                  condition.append(s"""${x._1} = '${x._2}'""")
                  rawDf = rawDf.withColumn(x._1, lit(x._2))
                }
                }
                rawDf.write.format("delta").option("path", outputPath).option("replaceWhere", condition.mkString(" AND ")).mode(writeMode).partitionBy(partitions: _*).saveAsTable(tableName)
              }
              case None => {
                if (!checkTable(spark, tableName))
                  rawDf.write.format("delta").option("path", outputPath).partitionBy(partitions: _*).saveAsTable(tableName)
                else {
                  replaceWhere match {
                    case Some(con) => rawDf.write.format("delta").option("path", outputPath).option("replaceWhere", con).mode(writeMode).partitionBy(partitions: _*).saveAsTable(tableName)
                    case None => {
                      if (writeMode == "overwrite") {
                        rawDf.persist()
                        val partCols = rawDf.selectExpr(partitions: _*).distinct.collect()
                        val con = partCols.map(part => partitions.map(x => s"$x = '${part.getAs[String](x)}'").mkString(" AND ")).map(x => s"($x)").mkString(" OR ")
                        rawDf.write.format("delta").option("path", outputPath).option("replaceWhere", con).mode(writeMode).partitionBy(partitions: _*).saveAsTable(tableName)
                      } else {
                        rawDf.write.format("delta").option("path", outputPath).mode(writeMode).partitionBy(partitions: _*).saveAsTable(tableName)
                      }
                    }
                  }
                }
              }
            }
          }
          case None => rawDf.write.format("delta").option("path", outputPath).mode(writeMode).saveAsTable(tableName)
        }
        storeOperationStats()

        val distinctPartition = getDistinctPartitions(partitionKeys, rawDf);
        if (distinctPartition.nonEmpty) {
          updateAndStoreStatus(PipelineNodeStatus.RegisterLensPart, updateDB = true)
          logger.info(s"Moved to RegisterLensPart state")
          println(s"INFO: Moved to RegisterLensPart state")
          var isComplete = true
          var retry: Int = 1
          while (isComplete) {
            try {
              registerLensPartitions(this.tableName, this.databaseName, distinctPartition)
              isComplete = false
            } catch {
              case exception: Exception => {
                logger.warn(s"Received exception while registering partitions ${retryCount}")
                exception.printStackTrace()
                if (retry == 4) {
                  exception.printStackTrace()
                  throw exception
                }
                Thread.sleep(retry * 2000)
                retry = +1
              }
            }
          }
        }
        updateAndStoreStatus(PipelineNodeStatus.Finished, updateDB = true)
        flag = false
      } // end try
      catch {
        case modException: DeltaConcurrentModificationException => {
          retryCount += 1
          logger.warn(s"Got Concurrent Modification Exception. Rerunning ${modException.getMessage}")
          if (retryCount == 3) {
            throw new Exception("Concurrent Modification Exception, Tried thrice...")
          }
          Thread.sleep(500)
        }
        case interruptedException: InterruptedException => {
          logger.warn(s"InterruptedException Exception.")
          updateRetryableExceptionStatus(PipelineNodeStatus.EVENT_TODO, interruptedException)
        }
        case sparkException: SparkException => {
          logger.warn(s"SparkException Exception.")
          updateRetryableExceptionStatus(PipelineNodeStatus.EVENT_TODO, sparkException)
        }
        case exception: Exception => {
          logger.error("got exception in " + taskName)
          println(s"exception $exception")
          logger.error(exception.getMessage, exception)
          updateAndStoreStatus(PipelineNodeStatus.Error, exception)
          flag = false
        }
      } // end catch
    }
  }

  def getDistinctPartitions(partitionKeys: Option[String], rawDf: DataFrame): String = {
    var partitionStr = ""
    partitionKeys match {
      case Some(partition) => {
        val partitions = partition.split("\\|").toSeq
        val processTimePartn = partitions.filter(_.contains("process_time"))
        if (processTimePartn.nonEmpty && Set("ssp", "programmatics").contains(databaseName)) {
          val tsEncoder = rawDf.sparkSession.implicits.newTimeStampEncoder
          val distinctPart = rawDf.selectExpr(processTimePartn: _*).distinct.map(_.getAs[Timestamp](0))(tsEncoder)
            .collect().head
          logger.info(s"INFO: distinct partitions ${distinctPart} ")
          println(s"INFO: distinct partitions ${distinctPart} ")
          partitionStr = distinctPart.toString()
        }
      }
      case None => {
        partitionStr = ""
      }
    }
    partitionStr
  }
}


class DeltaWriter extends BaseWriter {

  def append(rawDf: DataFrame, tableOrLocation: String, tableName: String, saveLocation: String): Unit = {
    if (tableOrLocation == "table")
      rawDf.write.format("delta").mode("append").saveAsTable(tableName)
    else
      rawDf.write.format("delta").mode("append").save(saveLocation)

  }

  def overwrite(rawDf: DataFrame, tableOrLocation: String, tableName: String, saveLocation: String): Unit = {
    if (tableOrLocation == "table")
      rawDf.write.format("delta").mode("append").saveAsTable(tableName)
    else
      rawDf.write.format("delta").mode("append").save(saveLocation)
  }

  def overWriteReplaceWhere(rawDf: DataFrame, tableOrLocation: String, tableName: String, saveLocation: String, replaceWhereCondition: String): Unit = {
    if (tableOrLocation == "table") {
      rawDf.write.format("delta")
        .option("replaceWhere", replaceWhereCondition)
        .mode("overwrite")
        .saveAsTable(tableName)
    }
    else {
      rawDf.write.format("delta")
        .option("replaceWhere", replaceWhereCondition)
        .mode("overwrite")
        .save(saveLocation)

    }
  }

  def merge(targetTable: String, upodatesDF: DataFrame, match_conditions: String, delete_condiotion: String, updateCondition: String, insetColumns: Map[String, String], updateColumns: Map[String, String]): Unit = {

    val deltaTableTarget = DeltaTable.forName(targetTable)

    var mergeObject = deltaTableTarget
      .as("target")
      .merge(
        sourceDF.as("updates"),
        match_conditions)

    // delete
    if (delete_condiotion != null) {
      mergeObject = mergeObject.whenMatched(delete_condiotion).delete()
    }
    // update

    if (updateColumns != null)
      mergeObject = mergeObject.whenMatched().updateExpr(updateColumns)
    else
      mergeObject = mergeObject.whenMatched().updateAll()


    if (insetColumns != null)
      mergeObject = mergeObject.whenNotMatched().insertExpr(insetColumns)
    else
      mergeObject = mergeObject.whenNotMatched().insertAll()

    mergeObject.execute()


  }


  override def call(): Any = {
    var flag = true
    var retryCount: Int = 0
    while (flag) {
      try {
        updateAndStoreStatus(PipelineNodeStatus.Started)
        logger.info("INFO: Delta writer started")
        var rawDf = this.inputDataframe(PROCESSEDDF)

        val tableName = s"""${this.databaseName}.${this.tableName}"""
        val saveLocation = s"${this.outputPath}"
        ""

        //        val partitionKeys = this.writerOptions.get("partitionKeys")
        //        val partitionValues = this.writerOptions.get("partitionValues")
        //        val replaceWhere = this.writerOptions.get("replaceWhere")
        ////        val writeMode = this.writerOptions.get("mode") match {
        //          case Some(value) => value.toLowerCase match {
        //            case "append" => "append"
        //            case _ => "overwrite"
        //          }
        //          case None => "overwrite"
        //        }

        // This is to change as per writing options
        val tableOrLocation = "table"
        var writeMode: String = null

        if (this.writerOptions != null)
          writeMode = this.writerOptions.get("mode").toString
        else
          writeMode = "overwrite"


        val targetTable = null
        val match_condition = null
        val delete_condition= null
        val update_condition = null
        val insetColumns = null
        val updateColumns = null
        val replaceWhereCondition = null;

        if (writeMode == "append")
          append(rawDf, tableOrLocation, tableName, saveLocation)
        else if (writeMode == "merge")
          merge(targetTable, rawDf, match_condition, delete_condition, update_condition, insetColumns , updateColumns)
        else if (writeMode == "replaceWhere")
          overWriteReplaceWhere(rawDf, tableOrLocation, tableName, saveLocation,replaceWhereCondition)
        else
          overwrite(rawDf, tableOrLocation, tableName, saveLocation)


        updateAndStoreStatus(PipelineNodeStatus.Finished, updateDB = true)
        flag = false
      } // end try
      catch {
        case modException: DeltaConcurrentModificationException => {
          retryCount += 1
          logger.warn(s"Got Concurrent Modification Exception. Rerunning ${modException.getMessage}")
          if (retryCount == 3) {
            throw new Exception("Concurrent Modification Exception, Tried thrice...")
          }
          Thread.sleep(500)
        }
        case interruptedException: InterruptedException => {
          logger.warn(s"InterruptedException Exception.")
          updateRetryableExceptionStatus(PipelineNodeStatus.EVENT_TODO, interruptedException)
        }
        case sparkException: SparkException => {
          logger.warn(s"SparkException Exception.")
          updateRetryableExceptionStatus(PipelineNodeStatus.EVENT_TODO, sparkException)
        }
        case exception: Exception => {
          logger.error("got exception in " + taskName)
          println(s"exception $exception")
          logger.error(exception.getMessage, exception)
          updateAndStoreStatus(PipelineNodeStatus.Error, exception)
          flag = false
        }
      } // end catch
    }
  }

  def getDistinctPartitions(partitionKeys: Option[String], rawDf: DataFrame): String = {
    var partitionStr = ""
    partitionKeys match {
      case Some(partition) => {
        val partitions = partition.split("\\|").toSeq
        val processTimePartn = partitions.filter(_.contains("process_time"))
        if (processTimePartn.nonEmpty && Set("ssp", "programmatics").contains(databaseName)) {
          val tsEncoder = rawDf.sparkSession.implicits.newTimeStampEncoder
          val distinctPart = rawDf.selectExpr(processTimePartn: _*).distinct.map(_.getAs[Timestamp](0))(tsEncoder)
            .collect().head
          logger.info(s"INFO: distinct partitions ${distinctPart} ")
          println(s"INFO: distinct partitions ${distinctPart} ")
          partitionStr = distinctPart.toString()
        }
      }
      case None => {
        partitionStr = ""
      }
    }
    partitionStr
  }
}

