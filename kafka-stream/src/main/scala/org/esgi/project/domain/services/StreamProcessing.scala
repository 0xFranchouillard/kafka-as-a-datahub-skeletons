package org.esgi.project.domain.services

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.esgi.project.domain.models.Trade

import java.time.Duration
import java.util.Properties
import org.slf4j.{Logger, LoggerFactory}

object StreamProcessing extends PlayJsonSupport {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"esgi-iabd-binance-kafka-stream"
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // TODO: All the code will be under the builder
  val tradesTopicName: String = "trades"
  val tradesByPairByMinStoreName: String = "TradesByPairByMin"

  implicit val tradeSerde: Serde[Trade] = toSerde[Trade]

  val trades: KStream[String, Trade] = builder.stream[String, Trade](tradesTopicName)
  logger.info("test 0")
  val tradesByPair: KGroupedStream[String, Trade] = trades.groupBy((_, trade) => trade.pair)
  logger.info("test 1")
  val tradesByPairByMin: KTable[Windowed[String], Long] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .count()(Materialized.as(tradesByPairByMinStoreName))
  logger.info("test 2")

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

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
