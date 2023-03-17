package org.esgi.project.domain.models

import java.time.Instant

case class TradesByPairByMin(eventTime: Instant, pair: String, price: Double, quantity: Double) {
  def apply(eventTime: Instant, pair: String, price: String, quantity: String): TradesByPairByMin = {
    val priceInDouble = convertStringToDouble(price)
    val quantityInDouble = convertStringToDouble(quantity)
    TradesByPairByMin(eventTime, pair, priceInDouble, quantityInDouble)
  }
  private def convertStringToDouble(str: String): Double = {
    str.toDouble
  }
}

object TradesByPairByMin {
  implicit val format: OFormat[Metric] = Json.format[Metric]
}
