package com.ibm.mq.spark.core.interfaces

import com.ibm.mq.spark.core.audit.AuditEvent

trait AuditSink extends AutoCloseable {

  def emit(event: AuditEvent): Unit

  def flush(): Unit

  override def close(): Unit
}
