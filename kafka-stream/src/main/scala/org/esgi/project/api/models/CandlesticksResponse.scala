package org.esgi.project.api.models

import org.esgi.project.domain.models.StockExchange
import play.api.libs.json.{Json, OFormat}

import java.security.Timestamp
import java.time.OffsetDateTime

case class StockExchangeByPair(
                                 pair: String,
                                 stockExchange: StockExchange
                               )

case class StockExchangeByPairInWindow(
                                         timestamp: OffsetDateTime,
                                         stocksExchange: List[StockExchangeByPair]
                                       )

case class VolumeByPair(
                                pair: String,
                                volume: Double
                              )

case class VolumeByPairInWindow(
                                        timestamp: OffsetDateTime,
                                        volumes: List[VolumeByPair]
                                      )

case class CandlesticksResponse (
    openingPrice: Double,
    closingPrice: Double,
    lowestPrice: Double,
    highestPrice: Double,
    volume: Double,
    timestampValue: OffsetDateTime
                                )

case class CandlesticksResponseList (
    pair: String,
    value: List[CandlesticksResponse]
                                )

object StockExchangeByPair {
  implicit val format: OFormat[StockExchangeByPair] = Json.format[StockExchangeByPair]
}

object StockExchangeByPairInWindow {
  implicit val format: OFormat[StockExchangeByPairInWindow] = Json.format[StockExchangeByPairInWindow]
}

object VolumeByPair {
  implicit val format: OFormat[VolumeByPair] = Json.format[VolumeByPair]
}

object VolumeByPairInWindow {
  implicit val format: OFormat[VolumeByPairInWindow] = Json.format[VolumeByPairInWindow]
}

object CandlesticksResponse {
  implicit val format: OFormat[CandlesticksResponse] = Json.format[CandlesticksResponse]
}

object CandlesticksResponseList {
  implicit val format: OFormat[CandlesticksResponseList] = Json.format[CandlesticksResponseList]
}