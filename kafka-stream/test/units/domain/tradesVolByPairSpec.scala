package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.domain.models.Trade
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest.FunSuite

import java.lang
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

case class tradesVolByPairSpec(tradesVolByPair: WindowStore[String, ValueAndTimestamp[Double]])
  extends FunSuite with PlayJsonSupport {
  def assertTradesVolByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the volume of trades by pair per minutes
    val tradesVolByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val tradesVol = trade.map(_.quantity).sum
        (pair, tradesVol)
      }

    tradesVolByPair.foreach { case (pair, tradesVol) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Double]]] =
        this.tradesVolByPair
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == tradesVol)
        case None => assert(false, s"No data for $pair in ${this.tradesVolByPair.name()}")
      }
    }
  }

  def assertTradesVolByPairPerHours(trades: List[Trade]): Unit = {
    // Assert the volume of trades by pair per hours
    val tradesVolByPairH: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val tradesVol = trade.map(_.quantity).sum
        (pair, tradesVol)
      }

    tradesVolByPairH.foreach { case (pair, tradesVol) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Double]]] =
        this.tradesVolByPair
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == tradesVol)
        case None => assert(false, s"No data for $pair in ${this.tradesVolByPair.name()}")
      }
    }
  }
}

object tradesVolByPairSpec {
  def initMin(testDriver: TopologyTestDriver): tradesVolByPairSpec = {
    val tradesVolByPairPerMin: WindowStore[String, ValueAndTimestamp[Double]] =
      testDriver.getTimestampedWindowStore[String, Double](
        StreamProcessing.tradesVolByPairPerMinStoreName
      )
    tradesVolByPairSpec(tradesVolByPairPerMin)
  }

  def initHours(testDriver: TopologyTestDriver): tradesVolByPairSpec = {
    val tradesVolByPairPerHours: WindowStore[String, ValueAndTimestamp[Double]] =
      testDriver.getTimestampedWindowStore[String, Double](
        StreamProcessing.tradesVolByPairPerHourStoreName
      )
    tradesVolByPairSpec(tradesVolByPairPerHours)
  }
}
