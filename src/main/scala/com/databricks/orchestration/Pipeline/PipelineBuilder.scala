package com.databricks.orchestration.Pipeline

import com.databricks.orchestration.commons.{PipeLineType, Task}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class PipelineBuilder {


  private var pipelineGraph: PipelineGraph = new PipelineGraph()
  private var pipeline: Pipeline = new Pipeline(pipelineGraph)
  private var pName: String = ""
  private var pipelineId: String = ""
  private var runId: String = ""
  private var productName: String = ""
  private var topicName: String = ""
  private var inputPath: String = ""
  private var outputPath: String = ""
  private var batchId: String = ""
  private var writerOptions: Map[String,String]  = null
  private var readerOptions: Map[String,String]  = null
  private var processorOptions: Map[String,String]  = null
  private var tableName: String = ""
  private var databaseName: String = ""
  private var partitionSize: Int = 0
  private var pipelineDefId: String = ""
  private var pipelineType: PipeLineType.Value = PipeLineType.CDC

  def addSparkSession(sparkSession: SparkSession): PipelineBuilder = {
    pipeline.sparkSession = sparkSession
    (this)
  }

  def setBatchId(batchId: String): PipelineBuilder = {
    this.batchId = batchId
    this
  }

  def setPipelineType(pipelineType: PipeLineType.Value): PipelineBuilder = {
    this.pipelineType = pipelineType
    this
  }

  def setProductName(productName: String): PipelineBuilder = {
    this.productName = productName
    this
  }

  def setTopicName(topicName: String): PipelineBuilder = {
    this.topicName = topicName
    this
  }

  def setInputPath(inputPath: String): PipelineBuilder = {
    this.inputPath = inputPath
    this
  }

  def setOutputPath(outputPath: String): PipelineBuilder = {
    this.outputPath = outputPath
    this
  }

  def setPipelineDefId(pipelineDefId: String): PipelineBuilder = {
    this.pipelineDefId = pipelineDefId
    this
  }

  def setPartitionSize(partitionSize: Int): PipelineBuilder = {
    this.partitionSize = partitionSize
    this
  }

  def setReaderOptions(options: Map[String,String]): PipelineBuilder = {
    this.readerOptions = options
    this
  }

  def setProcessorOptions(options: Map[String,String]): PipelineBuilder = {
    this.processorOptions = options
    this
  }

  def setWriterOptions(options: Map[String,String]): PipelineBuilder = {
    this.writerOptions = options
    this
  }

  def setTableName(tableName: String): PipelineBuilder = {
    this.tableName = tableName
    this
  }

  def setDatabaseName(databaseName: String): PipelineBuilder = {
    this.databaseName = databaseName
    this
  }

  def setBasicInfo(task: Task): Task = {
    task.pipelineName = pName
    task.pipeLineUid = pipelineId
    task.taskStatus = PipelineNodeStatus.None
    task.runId = runId
    task.productName = productName
    task.logger = Logger.getLogger(task.getClass)
    task.pipelineDefId = pipelineDefId

//    task.topicName = topicName
//    task.inputPath = inputPath
//    task.outputPath = outputPath
//    task.batchId = batchId
//
//    task.tableName = tableName
//    task.partitionSize = partitionSize
//
//    task.readerOptions = readerOptions
//    task.processorOptions = processorOptions
//    task.writerOptions = writerOptions
//    task.databaseName = databaseName
    task
  }

  def addTask(key: String, task: Task): PipelineBuilder = {
    setBasicInfo(task)
    val pipelineNode = createPipelineNode(key, task)
    pipelineGraph.add_node(pipelineNode)
    (this)
  }


  def addAfter(afterNodeKey: String, key: String, task: Task): PipelineBuilder = {
    setBasicInfo(task)
    val pipelineNode = createPipelineNode(key, task)
    pipelineGraph.add_node(pipelineNode)
    val previousNode = pipelineGraph.get_node(afterNodeKey)
    pipelineGraph.add_Edge(previousNode, pipelineNode)
    this
  }

  def createPipelineNode(key: String, task: Task): PipelineNode = {
    if (pipelineGraph.nodes.contains(key)) {
      return pipelineGraph.get_node(key)
    }
    setBasicInfo(task)
    var pipelineNode: PipelineNode = new PipelineNode(key, task)
    (pipelineNode)
  }

  def build(): Pipeline = {
    // all generic validation can go here
    (pipeline)
  }

  def setPipelineName(name: String): PipelineBuilder = {
    this.pName = name
    pipelineGraph.pipeLineName = name

    def uuid = java.util.UUID.randomUUID.toString

    this.pipelineId = uuid
    this
  }

  def setRunId(id: String): PipelineBuilder = {
    this.runId = id
    this
  }


}


object PipelineBuilder {
  def start(): PipelineBuilder = {
    new PipelineBuilder()
  }
}
