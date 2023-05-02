package org.esgi.project.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json._

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
object WebServer extends PlayJsonSupport {
  import akka.http.scaladsl.server.Route

  def routes(streams: KafkaStreams): Route = {

    concat(
      path("hello") {
        get {
          complete("Hello world")
        }
      },
      //GET /trades/:pair/stats

      // return "pair": "ETHUSDT", "trades_over_last_hour": 200, "volume_over_last_hour": 28, "average_price_over_last_hour": 1800
      path("trades"/ Segment /"Stats") { pair =>
        get {
          val api = new API(streams)
          val from = OffsetDateTime.now().minusHours(1)
          val to = OffsetDateTime.now()
          println(to)
          val res = api.getTradesStats(from, to, pair)
          val test = Json.toJson(res)
          val testString = Json.stringify(test)
          complete(testString)
          }
        },
      //GET /trades/:pair/candles?from=??&to=??
      path("trades"/ Segment /"candles") { pair =>
        get {
          parameters(Symbol("from").as[String], Symbol("to").as[String]) { (from, to) =>
            println(from)
//            val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy_HH:mm:ss")

            val formatter = new DateTimeFormatterBuilder()
              .appendPattern("dd/MM/yyyy_HH:mm:ss")
              .toFormatter()
              .withZone(ZoneOffset.UTC)

            val api = new API(streams)
            val res = api.getTradesCandles(OffsetDateTime.parse(from, formatter), OffsetDateTime.parse(to, formatter), pair)
            val test = Json.toJson(res)
            val testString = Json.stringify(test)
            complete(testString)
          }
        }
      }
    )
  }
}