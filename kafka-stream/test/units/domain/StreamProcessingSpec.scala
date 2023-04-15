package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.domain.models.{MeanPriceByPairPerMin, StockExchange, Trade, TradeInput}
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest._

import java.lang
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.Calendar
import scala.util.Random
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends FunSuite with PlayJsonSupport {
  import StreamProcessingSpec.Converters._

  test("Validate advanced statistics computation") {
    val count = 5 + Random.nextInt(25)
    val trades: List[Trade] = (1 to count)
      .map { _ =>
        val dt = Calendar.getInstance().getTime
          Trade(
            "trade",
            dt,
            "BTCUSDT",
            Random.nextLong(9999999999L),
            Random.nextDouble(),
            Random.nextDouble(),
            Random.nextLong(99999999999L),
            Random.nextLong(99999999999L),
            dt,
            Random.nextBoolean(),
            Random.nextBoolean()
          )
    }.toList

    // When
    val testDriver: TopologyTestDriver =
      new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val tradePipe = testDriver.createInputTopic(
      StreamProcessing.tradesTopicName,
      Serdes.stringSerde.serializer,
      toSerde[TradeInput].serializer
    )

    tradePipe.pipeRecordList(trades.map(_.toTradeInput.toTestRecord).asJava)

    // Then
    // Assert the count of trades by pair per minutes
    val tradesByPair: Map[String, Int] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) => (pair, trade.size) }

    val tradesByPairByMin: WindowStore[String, ValueAndTimestamp[Long]] =
      testDriver.getTimestampedWindowStore[String, Long](StreamProcessing.tradesByPairByMinStoreName)

    tradesByPair.foreach { case (pair, count) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Long]]] =
        tradesByPairByMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == count)
        case None      => assert(false, s"No data for $pair in ${tradesByPairByMin.name()}")
      }
    }

    // Assert the mean price by pair per minutes
    val meanPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val meanPrice = trade.map(_.price).sum / trade.size
        (pair, meanPrice)
      }

    val meanPriceByPairPerMin: WindowStore[String, ValueAndTimestamp[MeanPriceByPairPerMin]] =
      testDriver.getTimestampedWindowStore[String, MeanPriceByPairPerMin](
        StreamProcessing.meanPriceByPairPerMinStoreName
      )

    meanPriceByPair.foreach { case (pair, meanPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[MeanPriceByPairPerMin]]] =
        meanPriceByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().meanPrice == meanPrice)
        case None      => assert(false, s"No data for $pair in ${meanPriceByPairPerMin.name()}")
      }
    }

    // Assert the max price by pair per minutes
    val maxPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val maxPrice = trade.map(_.price).max
        (pair, maxPrice)
      }

    val tradesCandlestickByPairPerMin: WindowStore[String, ValueAndTimestamp[StockExchange]] =
      testDriver.getTimestampedWindowStore[String, StockExchange](
        StreamProcessing.stockExchangeByPairPerMin
      )

    maxPriceByPair.foreach { case (pair, maxPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        tradesCandlestickByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().maxPrice == maxPrice)
        case None => assert(false, s"No data for $pair in ${tradesCandlestickByPairPerMin.name()}")
      }
    }

    // Assert the min price by pair per minutes
    val minPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val minPrice = trade.map(_.price).min
        (pair, minPrice)
      }

    minPriceByPair.foreach { case (pair, minPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        tradesCandlestickByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().minPrice == minPrice)
        case None => assert(false, s"No data for $pair in ${tradesCandlestickByPairPerMin.name()}")
      }
    }

    // Assert the opening price by pair per minutes
    val openingPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val openingPrice = trade.map(_.price).head
        (pair, openingPrice)
      }

    openingPriceByPair.foreach { case (pair, openingPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        tradesCandlestickByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().openingPrice == openingPrice)
        case None => assert(false, s"No data for $pair in ${tradesCandlestickByPairPerMin.name()}")
      }
    }

    // Assert the ending price by pair per minutes
    val endingPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val endingPrice = trade.map(_.price).last
        (pair, endingPrice)
      }

    endingPriceByPair.foreach { case (pair, endingPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[StockExchange]]] =
        tradesCandlestickByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().endingPrice == endingPrice)
        case None => assert(false, s"No data for $pair in ${tradesCandlestickByPairPerMin.name()}")
      }
    }

    // Assert the volume of trades by pair per minutes
    val tradesVolByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val tradesVol = trade.map(_.quantity).sum
        (pair, tradesVol)
      }

    val tradesVolByPairPerMin: WindowStore[String, ValueAndTimestamp[Double]] =
      testDriver.getTimestampedWindowStore[String, Double](
        StreamProcessing.tradesVolByPairPerMinStoreName
      )

    tradesVolByPair.foreach { case (pair, tradesVol) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Double]]] =
        tradesVolByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == tradesVol)
        case None => assert(false, s"No data for $pair in ${tradesVolByPairPerMin.name()}")
      }
    }

    // Assert the volume of trades by pair per hours
    val tradesVolByPairH: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val tradesVol = trade.map(_.quantity).sum
        (pair, tradesVol)
      }

    val tradesVolByPairPerHour: WindowStore[String, ValueAndTimestamp[Double]] =
      testDriver.getTimestampedWindowStore[String, Double](
        StreamProcessing.tradesVolByPairPerHourStoreName
      )

    tradesVolByPairH.foreach { case (pair, tradesVol) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Double]]] =
        tradesVolByPairPerHour
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == tradesVol)
        case None => assert(false, s"No data for $pair in ${tradesVolByPairPerHour.name()}")
      }
    }
  }
}

object StreamProcessingSpec {
  object Converters {
    implicit class TradeToTradeInput(trade: Trade) {
      def toTradeInput: TradeInput =
        new TradeInput(
          trade.eventType,
          trade.eventTime,
          trade.pair,
          trade.tradeId,
          trade.price.toString,
          trade.quantity.toString,
          trade.buyerOrderId,
          trade.sellerOrderId,
          trade.tradeTime,
          trade.isBuyerMaker,
          trade.isBestMatch
        )
    }
    implicit class VisitToTestRecord(trade: TradeInput) {
      def toTestRecord: TestRecord[String, TradeInput] =
        new TestRecord[String, TradeInput](trade.s, trade, trade.E.toInstant)
    }
  }
}