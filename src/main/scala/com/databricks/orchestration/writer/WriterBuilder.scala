package com.databricks.orchestration.writer

import org.apache.log4j.Logger


class WriterBuilder {
  val logger = Logger.getLogger(getClass)

  /**
   * Writes/Updates to 'outputPath' and creates table 'databaseName'.'tableName'
   * Other optional writerOptions need to set depending upon following usecases -
   * 1. Overwrite / Append to Non partition tables. - set options 'mode'
   * 2. Overwrite static partition  - set options 'partitionKeys' and 'partitionValues'
   *    For multiple columns use '|' seprated list in order.
   * 3. Dynamic partition with conditional partition overwrite - set options 'partitionKeys' and 'replaceWhere'
   * 4. Dynamic partition with auto populate condition - set options 'partitionKeys'
   */

  def getDeltaWriter: BaseWriter = {
    logger.info("batch delta writer scd4 created")
    new DeltaWriter()
  }
}


object WriterBuilder {
  def start(): WriterBuilder ={
    new WriterBuilder()
  }
}