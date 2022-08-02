package com.databricks.orchestration.reader

import org.apache.log4j.Logger


class ReaderBuilder {
  val logger: Logger = Logger.getLogger(getClass)

  def getParquetReader: BaseReader = {
    logger.info("Parquet Reader Created")
    new ParquetReader()
  }

  def getORCReader: BaseReader = {
    logger.info("ORC Reader Created")
    new ORCReader()
  }

  def getAvroReader: BaseReader = {
    logger.info("Avro Reader Created")
    new AvroReader()
  }

  def getJSONReader: BaseReader = {
    logger.info("JSON Reader Created")
    new JSONReader()
  }

  def getCSVReader: BaseReader = {
    logger.info("CSV Reader Created")
    new CsvReader()
  }

  /**
   * To use snowflake Reader, Need to configure some env variables.
   * snowflake_username, snowflake_password, snowflake_url,snowflake_warehouse
   * If need to read entire table - Set properties in readerOptions - sfDbName, sfSchemaName, sfTableName
   * If need to read using SF query - Set properties in readerOptions - sfDbName, sfSchemaName, sfQuery
   */
  def getSnowflakeReader: BaseReader = {
    logger.info("SnowFlake Reader Created")
    new SnowflakeReader()
  }
}


object ReaderBuilder
{
  def start(): ReaderBuilder ={
    new ReaderBuilder()
  }

}



