package com.databricks.orchestration.reader
import com.databricks.orchestration.commons.{Options, Task}

abstract class BaseReader  extends  Task{
  var options:Options = null
}
