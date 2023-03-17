package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}

import java.time.Instant

case class Trade(
    eventType: Instant,
    eventTime: Instant,
    pair: String,
    tradeId: Integer,
    price: Double,
    quantity: Double,
    buyerOrderId: Integer,
    sellerOrderId: Integer,
    tradeTime: Instant,
    isBuyerMaker: Boolean,
    isBestMatch: Boolean
) {
  def apply(
      eventType: Instant,
      eventTime: Instant,
      pair: String,
      tradeId: Integer,
      price: String,
      quantity: String,
      buyerOrderId: Integer,
      sellerOrderId: Integer,
      tradeTime: Instant,
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

object Trade {
  implicit val format: OFormat[Trade] = Json.format[Trade]
}
