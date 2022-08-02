package com.databricks.orchestration.utility


import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession, functions}
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.util.Try

object Utility {

  private val logger = Logger.getLogger(getClass)

  /**
   *
   * @param df      Input dataframe
   * @param alias   alias column name after hashing column(s)
   * @param numBits The numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256)
   * @param cols    Names of the column/list to be hashed
   * @return returns the dataframe with new hashed column
   */

  def hashIt(df: DataFrame, alias: String, numBits: Int, cols: String*): DataFrame = {
    val concatString = cols.foldLeft(lit(""))((x, y) => concat(x, coalesce(col(y).cast("string"), lit("n/a"))))
    df.withColumn(alias, sha2(concatString, numBits))
  }

  def hashItComplete(df: DataFrame, numBits: Int, cols: String*): DataFrame = {
    val newDf = cols.foldLeft(df) {
      case (tmpdf, columnName) => tmpdf.withColumn(s"${columnName}_hash", sha2(coalesce(col(columnName).cast("string"), lit("n/a")), numBits))
    }
    newDf
  }

  def hashNumericalIt(df: DataFrame, alias: String, cols: Column*): DataFrame = {
    val concatString = cols.foldLeft(lit(""))((x, y) => concat(x, coalesce(y.cast("string"), lit("n/a"))))
    df.withColumn(alias, abs(hash(concatString)).mod(500))
  }

  /**
   *
   * @param df   input dataframe
   * @param cols Names of the column/list to be droped
   * @return returns the dataframe after droping the columns
   */

  def dropIt(df: DataFrame, cols: String*): DataFrame = {

    val newDf = cols.foldLeft(df) {
      case (tmpdf, columnName) => tmpdf.drop(columnName)
    }
    newDf

  }

  /**
   * This fucntion helps to check if column is present in dataframe or not
   *
   * @param df      input dataframe
   * @param colName column name which has to be checked
   * @return Boolean
   */

  def hasColumn(df: DataFrame, colName: String): Boolean = Try(df(colName)).isSuccess


  /**
   * This function helps in hashing the test
   *
   * @param text String which has to be hashed
   * @return hashed string
   */
  def sha256Hash(text: String): String = {
    String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))
  }

  /**
   * This function helps in identifying if the column is present or not
   *
   * @param inputColumns   input set of columns out of which we need to identify
   * @param colsToIdentify columns which has to be identified. If Boolean is true then we do exact match. If not we do regex match
   * @return returns the Array of column names which are matching
   */
  def identifyColumns(inputColumns: Array[String], colsToIdentify: Array[(String, Boolean)]): Array[String] = {

    val columnsListBuffer = scala.collection.mutable.ArrayBuffer[String]()
    colsToIdentify.foldLeft(columnsListBuffer) {
      case (z, i) => if (i._2) {
        if (inputColumns.contains(i._1)) {
          z += i._1
        }
      } else {
        z ++= inputColumns.filter(_.contains(i._1)).toBuffer
      }
        z
    }
    columnsListBuffer.toArray
  }

  /**
   * This Functions helps you generate join condition based on the keys provided for target and updates
   *
   * @param joinKeys           is a Seq[String] can be passed as Seq("id") or Seq("id","name = empName")
   * @param extraJoinCondition extra join condition
   * @return returns join condition build
   */
  def buildInnerJoinCondition(joinKeys: Seq[String], extraJoinCondition: Option[String]): Column = {
    val innerJoinConditions = joinKeys.map(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        arr(1) match {
          case "<" => col(s"target.${arr(0)}") < col(s"updates.${arr(2)}")
          case "<=" => col(s"target.${arr(0)}") <= col(s"updates.${arr(2)}")
          case "=" => col(s"target.${arr(0)}") === col(s"updates.${arr(2)}")
          case ">=" => col(s"target.${arr(0)}") >= col(s"updates.${arr(2)}")
          case ">" => col(s"target.${arr(0)}") > col(s"updates.${arr(2)}")
          case "!=" => col(s"target.${arr(0)}") != col(s"updates.${arr(2)}")
        }
      }
      else if (arr.size == 1) {
        col(s"target.${arr(0)}") === col(s"updates.${arr(0)}")
      }
    })

    val innerJoinConditionsFinal = (extraJoinCondition match {
      case Some(value) => innerJoinConditions ++ List(expr(value))
      case None => innerJoinConditions
    }).asInstanceOf[List[Column]].reduce(_ && _)
    innerJoinConditionsFinal
  }

  def buildMergeJoinCondition(joinKeys: Seq[String], extraJoinCondition: Option[String]): (ListBuffer[String], ListBuffer[String], String) = {

    val targetTableCols: ListBuffer[String] = ListBuffer[String]()
    val updatesTableCols: ListBuffer[String] = ListBuffer[String]()
    joinKeys.foreach(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        targetTableCols += arr(0)
        updatesTableCols += arr(2)
      }
      else if (arr.size == 1) {
        targetTableCols += arr(0)
        updatesTableCols += arr(0)
      }
    })

    val union1: ListBuffer[String] = ListBuffer[String]()
    val union2: ListBuffer[String] = ListBuffer[String]()
    var mergeJoinConditionList: ListBuffer[String] = ListBuffer[String]()
    for (i <- updatesTableCols.indices) {
      union1 += s"null as mergekey$i"
      union2 += s"${updatesTableCols(i)} as mergekey$i"
    }

    for (i <- targetTableCols.indices) {
      mergeJoinConditionList += s"target.${targetTableCols(i)} = updates.mergekey$i"
    }

    union1 += "*"
    union2 += "*"
    println(mergeJoinConditionList)

    mergeJoinConditionList = extraJoinCondition match {
      case Some(value) => mergeJoinConditionList ++ List(value)
      case None => mergeJoinConditionList
    }
    val mergeJoinCondition = mergeJoinConditionList.mkString(" and ")
    println(mergeJoinConditionList)
    (union1, union2, mergeJoinCondition)

  }

  def extractJoinKeys(joinKeys: Seq[String]): ListBuffer[Column] = {
    val updatesTableCols: ListBuffer[Column] = ListBuffer[Column]()
    joinKeys.foreach(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        updatesTableCols += col(arr(2))
      }
      else if (arr.size == 1) {
        updatesTableCols += col(arr(0))
      }
    })
    updatesTableCols
  }

  def buildDPPForPartitionKeys(df: DataFrame, partitionKeys: Seq[String]): String = {

    var DPPString = ""
    partitionKeys.foreach(key => {
      val filter = (df.select(key).distinct().collect.map(row => row.getAs(key).toString)).mkString("'", "','", "'")
      DPPString = DPPString + s""" and target.$key in ($filter)"""
    })

    DPPString
  }

  def epochToTimestamp(epochMillis: Long): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+SSSS")
    df.format(epochMillis)
  }

  def checkTable(spark: SparkSession, tableName: String): Boolean = {
    val result = spark.catalog.tableExists(tableName)
    if (result) {
      logger.info(s"INFO: $tableName exists")
      println(s"INFO: $tableName exists")
    } else {
      logger.info(s"INFO: $tableName dosent exists")
      println(s"INFO: $tableName dosent exists")
    }
    result
  }
}
