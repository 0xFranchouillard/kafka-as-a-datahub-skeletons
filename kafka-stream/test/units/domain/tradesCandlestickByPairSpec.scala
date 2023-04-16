package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.domain.models.{StockExchange, Trade}
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest.FunSuite

import java.lang
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

case class tradesCandlestickByPairSpec(tradesCandlestickByPairPerMin:
                                             WindowStore[String, ValueAndTimestamp[StockExchange]])
  extends FunSuite with PlayJsonSupport {
  def assertMaxPriceByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the max price by pair per minutes
    val maxPriceByPair: Map[String, Double] = trades
      .groupBy (_.pair)
      .map { case (pair, trade) =>
        val maxPrice = trade.map (_.price).max
        (pair, maxPrice)
      }

    maxPriceByPair.foreach { case (pair, maxPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        this.tradesCandlestickByPairPerMin
          .fetch (
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some (row) => assert(row.value.value ().maxPrice == maxPrice)
        case None       => assert(false, s"No data for $pair in ${this.tradesCandlestickByPairPerMin.name ()}")
      }
    }
  }

  def assertMinPriceByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the min price by pair per minutes
    val minPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val minPrice = trade.map(_.price).min
        (pair, minPrice)
      }

    minPriceByPair.foreach { case (pair, minPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        this.tradesCandlestickByPairPerMin
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
        case Some(row) => assert(row.value.value().minPrice == minPrice)
        case None => assert(false, s"No data for $pair in ${this.tradesCandlestickByPairPerMin.name()}")
      }
    }
  }

  def assertOpeningPriceByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the opening price by pair per minutes
    val openingPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val openingPrice = trade.map(_.price).head
        (pair, openingPrice)
      }

    openingPriceByPair.foreach { case (pair, openingPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        this.tradesCandlestickByPairPerMin
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
        case Some(row) => assert(row.value.value().openingPrice == openingPrice)
        case None => assert(false, s"No data for $pair in ${this.tradesCandlestickByPairPerMin.name()}")
      }
    }
  }

  def assertEndingPriceByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the ending price by pair per minutes
    val endingPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val endingPrice = trade.map(_.price).last
        (pair, endingPrice)
      }

    endingPriceByPair.foreach { case (pair, endingPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        this.tradesCandlestickByPairPerMin
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
        case Some(row) => assert(row.value.value().endingPrice == endingPrice)
        case None => assert(false, s"No data for $pair in ${this.tradesCandlestickByPairPerMin.name()}")
      }
    }
  }
}

object tradesCandlestickByPairSpec {
  def init(testDriver: TopologyTestDriver): tradesCandlestickByPairSpec = {
    val tradesCandlestickByPairPerMin: WindowStore[String, ValueAndTimestamp[StockExchange]] =
      testDriver.getTimestampedWindowStore[String, StockExchange](
        StreamProcessing.stockExchangeByPairPerMin
      )
    tradesCandlestickByPairSpec(tradesCandlestickByPairPerMin)
  }
}
