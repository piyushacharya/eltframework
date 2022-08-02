package com.databricks.orchestration.writer

import com.databricks.orchestration.commons.{Options, Task}
import org.apache.spark.sql.DataFrame

abstract class BaseWriter extends  Task{
    var options:Options = null
    var sourceDF :DataFrame = null
    var sourceInMemoryTableName :String = null

}