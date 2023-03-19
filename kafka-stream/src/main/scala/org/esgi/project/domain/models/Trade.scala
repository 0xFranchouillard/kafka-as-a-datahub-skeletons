package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}

import java.time.OffsetDateTime

/* I receive this message by kafka:
  {"partition": 0,
   "offset": 1325351,
   "timestamp": "2023-03-18 20:28:20",
   "key": "BTCUSDT"
   "value": {
       "e": "trade",
       "E": 1679167701103,
       "s": "BTCUSDT",
       "t": 3014717088,
       "p": "27396.86000000",
       "q": "0.00037000",
       "b": 20340463959,
       "a": 20340463913,
       "T": 1679167701102,
       "m": false,
       "M": true
     }
   }
 */
case class Trade(
    eventType: OffsetDateTime,
    eventTime: OffsetDateTime,
    pair: String,
    tradeId: Int,
    price: Double,
    quantity: Double,
    buyerOrderId: Int,
    sellerOrderId: Int,
    tradeTime: OffsetDateTime,
    isBuyerMaker: Boolean,
    isBestMatch: Boolean
) {
  def apply(
      eventType: OffsetDateTime,
      eventTime: OffsetDateTime,
      pair: String,
      tradeId: Integer,
      price: String,
      quantity: String,
      buyerOrderId: Integer,
      sellerOrderId: Integer,
      tradeTime: OffsetDateTime,
      isBuyerMaker: Boolean,
      isBestMatch: Boolean
  ): Trade = {
    val priceInDouble = convertStringToDouble(price)
    val quantityInDouble = convertStringToDouble(quantity)
    Trade(
      eventType,
      eventTime,
      pair,
      tradeId,
      priceInDouble,
      quantityInDouble,
      buyerOrderId,
      sellerOrderId,
      tradeTime,
      isBuyerMaker,
      isBestMatch
    )
  }
  private def convertStringToDouble(str: String): Double = {
    str.toDouble
  }
}

case class TradeInput(
    e: String,
    E: OffsetDateTime,
    s: String
)

object Trade {
  // TODO: Blocked here because the message value's name are not binding with Trade case class
  //  (e != eventType, E != timestamp)
  implicit val format: OFormat[Trade] = Json.format[Trade]
}
