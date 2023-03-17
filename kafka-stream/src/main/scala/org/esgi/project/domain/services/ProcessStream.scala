package org.esgi.project.domain.services

import io.github.azhur.kafka.serde.PlayJsonSupport
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.scala.StreamsBuilder

object ProcessStream extends PlayJsonSupport {
  def processTradesByPairByMin(): Unit = {
    parseTrade()
    ...
    throw NotImplementedException
  }

  def processMeanPriceByPairByMin(): Unit = {
    throw NotImplementedException
  }

  def processCandleByPairByMin(): Unit = {
    throw NotImplementedException
  }

  def processVolByPairByMin(): Unit = {
    throw NotImplementedException
  }

  def processVolByPairByHour(): Unit = {
    throw NotImplementedException
  }
}
