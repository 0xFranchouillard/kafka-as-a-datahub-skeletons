package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.domain.models.{Trade, TradeInput}
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest._

import java.util.Calendar
import scala.util.Random
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends FunSuite with PlayJsonSupport {
  import StreamProcessingSpec.Converters._

  val count: Int = 5 + Random.nextInt(25)
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
  val tradePipe: TestInputTopic[String, TradeInput] = testDriver.createInputTopic(
    StreamProcessing.tradesTopicName,
    Serdes.stringSerde.serializer,
    toSerde[TradeInput].serializer
  )

  tradePipe.pipeRecordList(trades.map(_.toTradeInput.toTestRecord).asJava)

  // Then
  val tradeByPairPerMinTest: tradesByPairSpec =
    tradesByPairSpec.init(testDriver)

  test("Validate trades statistics computation") {
    tradeByPairPerMinTest.assertTradesByPairPerMin(trades)
  }

  val meanPriceByPairPerMinTest: meanPriceByPairSpec =
    meanPriceByPairSpec.init(testDriver)

  test("Validate trades mean price statistics computation") {
    meanPriceByPairPerMinTest.assertMeanPriceByPairPerMin(trades)
  }

  val tradesCandlestickByPairPerMinTest: tradesCandlestickByPairSpec =
    tradesCandlestickByPairSpec.init(testDriver)

  test("Validate trades candle-stick max statistics computation") {
    tradesCandlestickByPairPerMinTest.assertMaxPriceByPairPerMin(trades)
  }
  test("Validate trades candle-stick min statistics computation") {
    tradesCandlestickByPairPerMinTest.assertMinPriceByPairPerMin(trades)
  }
  test("Validate trades candle-stick opening statistics computation") {
    tradesCandlestickByPairPerMinTest.assertOpeningPriceByPairPerMin(trades)
  }
  test("Validate trades candle-stick ending statistics computation") {
    tradesCandlestickByPairPerMinTest.assertEndingPriceByPairPerMin(trades)
  }

  val tradesVolByPairPerMinTest: tradesVolByPairSpec =
    tradesVolByPairSpec.initMin(testDriver)

  test("Validate trades volume per minutes statistics computation") {
    tradesVolByPairPerMinTest.assertTradesVolByPairPerMin(trades)
  }

  val tradesVolByPairPerHoursTest: tradesVolByPairSpec =
    tradesVolByPairSpec.initHours(testDriver)

  test("Validate trades volume per hours statistics computation") {
//    tradesVolByPairPerHoursTest.assertTradesVolByPairPerHours(trades)
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