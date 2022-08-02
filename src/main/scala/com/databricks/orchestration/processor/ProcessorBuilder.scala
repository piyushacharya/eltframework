package com.databricks.orchestration.processor

import org.apache.log4j.Logger


class ProcessorBuilder {
  val logger: Logger = Logger.getLogger(getClass)

  def getColumnSelectorProcessor: BaseProcessor = {
    logger.info("ColumnSelector Processor Created")
    new ColumnSelectorProcessor()
  }

  def getSyncWithExistingTableProcessor: BaseProcessor = {
    logger.info("SyncWithExistingTableProcessor Processor Created")
    new SyncWithExistingTableProcessor()
  }
}


object ProcessorBuilder {
  def start(): ProcessorBuilder ={
    new ProcessorBuilder()
  }
}