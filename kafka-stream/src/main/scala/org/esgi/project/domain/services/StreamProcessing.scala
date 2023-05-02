package org.esgi.project.domain.services

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Printed, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.domain.models.{MeanPriceByPairPerMin, StockExchange, Trade, TradeInput}

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"esgi-iabd-binance-kafka-stream"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // TODO: All the code will be under the builder
  val tradesTopicName: String = "trades"

  val tradesByPairByMinStoreName: String = "TradesByPairByMinStore"
  val meanPriceByPairPerMinStoreName: String = "MeanPriceByPairPerMinStore"
  val stockExchangeByPairPerMin: String = "StockExchangeByPairPerMin"
  val tradesVolByPairPerMinStoreName: String = "TradesVolByPairPerMinStore"
  val tradesVolByPairPerHourStoreName: String = "TradesVolByPairPerHourStore"

  implicit val tradeInputSerde: Serde[TradeInput] = toSerde[TradeInput]
  implicit val tradeSerde: Serde[Trade] = toSerde[Trade]

  val tradesInput: KStream[String, TradeInput] = builder.stream[String, TradeInput](tradesTopicName)

  val trades: KStream[String, Trade] = tradesInput.mapValues(tradeInput =>
    Trade(
      eventType = tradeInput.e,
      eventTime = tradeInput.E,
      pair = tradeInput.s,
      tradeId = tradeInput.t,
      price = tradeInput.p,
      quantity = tradeInput.q,
      buyerOrderId = tradeInput.b,
      sellerOrderId = tradeInput.a,
      tradeTime = tradeInput.T,
      isBuyerMaker = tradeInput.m,
      isBestMatch = tradeInput.M
    )
  )

  //trades.print(Printed.toSysOut);

  val tradesByPair: KGroupedStream[String, Trade] = trades.groupBy((_, trade) => trade.pair)

  val tradesByPairByMin: KTable[Windowed[String], Long] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .count()(Materialized.as(tradesByPairByMinStoreName))

  //tradesByPairByMin.toStream.print(Printed.toSysOut);

  val meanPriceByPairPerMin: KTable[Windowed[String], MeanPriceByPairPerMin] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .aggregate[MeanPriceByPairPerMin](
      initializer = MeanPriceByPairPerMin.empty
    )(aggregator = { (_, trade, agg) =>
      agg.increment(trade.price)
    })(Materialized.as(meanPriceByPairPerMinStoreName))

  val tradesCandlestickByPairPerMin: KTable[Windowed[String], StockExchange] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .aggregate[StockExchange](
      initializer = StockExchange.empty
    )(aggregator = { (_, trade, agg) =>
      agg.increment(trade.price)
    })(Materialized.as(stockExchangeByPairPerMin))
  //tradesCandlestickByPairPerMin.toStream.print(Printed.toSysOut)
  val tradesVolByPairPerMin: KTable[Windowed[String], Double] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .aggregate[Double](
      initializer = 0L
    )(aggregator = { (_, trade, agg) =>
      agg + trade.quantity
    })(Materialized.as(tradesVolByPairPerMinStoreName))

  val tradesVolByPairPerHour: KTable[Windowed[String], Double] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)).advanceBy(Duration.ofHours(1))
    )
    .aggregate[Double](
      initializer = 0L
    )(aggregator = { (_, trade, agg) =>
      agg + trade.quantity
    })(Materialized.as(tradesVolByPairPerHourStoreName))
  //tradesVolByPairPerHour.toStream.print(Printed.toSysOut)
  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }
  def topology: Topology = builder.build()

  // auto loader from properties file in project
  def buildProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
