package com.other.test

import com.databricks.orchestration.Pipeline.PipelineBuilder
import com.databricks.orchestration.reader.{CsvReader, ReaderBuilder}
import com.databricks.orchestration.commons.Task
import com.databricks.orchestration.writer.WriterBuilder
import org.apache.spark.sql.SparkSession

object BasePipeLine {
  def main(args:Array[String]): Unit ={


    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      ).config("hive.metastore.warehouse.dir","/Users/piyush.acharya/MyWorkSpace/data")
      .getOrCreate()


    val csvtask =  ReaderBuilder.start().getCSVReader
    csvtask.readerOptions =Map("delimiter"->";")
    csvtask.inputPath="file:///Users/piyush.acharya/MyWorkSpace/workshops/data/data.csv"


    val deltaWriter = WriterBuilder.start().getDeltaWriter
    deltaWriter.databaseName="default"
    deltaWriter.tableName="company"

    val pipeline = PipelineBuilder.start()
      .addSparkSession(spark)
      .addTask("read_from_cdv",csvtask)
      .addAfter("read_from_cdv","store-data",deltaWriter).build()

    pipeline.start()

    spark.stop()


  }

}
