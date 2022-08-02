package com.other.test

import org.apache.spark.sql.SparkSession

object create_orch_db {

  var spark:SparkSession = null
  val  base_path ="/Users/piyush.acharya/MyWorkSpace/data/tables"
  def execute (  line: String): Unit =
  {
    val newline = line.replace("<base_path>",base_path)
    println(newline)
    spark.sql(newline)
  }
  def main(args: Array[String]): Unit = {


     spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      ).config("hive.metastore.warehouse.dir","/Users/piyush.acharya/MyWorkSpace/data")
      .getOrCreate()


    val ddl_file_path = "/Users/piyush.acharya/MyWorkSpace/workshops/Code/common-framework-master/target/classes/DDLs.sql";

    val lines = scala.io.Source.fromFile(ddl_file_path, "utf-8").getLines.mkString.split(";")
    lines.map(execute)


  }
}
