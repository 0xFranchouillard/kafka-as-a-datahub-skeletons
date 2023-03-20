package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}
import java.util.Date

case class Trade(
    eventType: String,
    eventTime: Date,
    pair: String,
    tradeId: Long,
    price: Double,
    quantity: Double,
    buyerOrderId: Long,
    sellerOrderId: Long,
    tradeTime: Date,
    isBuyerMaker: Boolean,
    isBestMatch: Boolean
)

object Trade {
  implicit val format: OFormat[Trade] = Json.format[Trade]

  def apply(
      eventType: String,
      eventTime: Date,
      pair: String,
      tradeId: Long,
      price: String,
      quantity: String,
      buyerOrderId: Long,
      sellerOrderId: Long,
      tradeTime: Date,
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
