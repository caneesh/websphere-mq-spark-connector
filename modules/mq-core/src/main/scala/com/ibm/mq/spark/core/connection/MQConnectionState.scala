package com.ibm.mq.spark.core.connection

sealed trait MQConnectionState {
  def isConnected: Boolean
  def canConnect: Boolean
  def name: String
}

object MQConnectionState {

  case object Disconnected extends MQConnectionState {
    override def isConnected: Boolean = false
    override def canConnect: Boolean = true
    override def name: String = "DISCONNECTED"
  }

  case object Connecting extends MQConnectionState {
    override def isConnected: Boolean = false
    override def canConnect: Boolean = false
    override def name: String = "CONNECTING"
  }

  case object Connected extends MQConnectionState {
    override def isConnected: Boolean = true
    override def canConnect: Boolean = false
    override def name: String = "CONNECTED"
  }

  case object Reconnecting extends MQConnectionState {
    override def isConnected: Boolean = false
    override def canConnect: Boolean = false
    override def name: String = "RECONNECTING"
  }

  case object Disconnecting extends MQConnectionState {
    override def isConnected: Boolean = false
    override def canConnect: Boolean = false
    override def name: String = "DISCONNECTING"
  }

  case class Failed(reason: String) extends MQConnectionState {
    override def isConnected: Boolean = false
    override def canConnect: Boolean = true
    override def name: String = "FAILED"
  }
}
