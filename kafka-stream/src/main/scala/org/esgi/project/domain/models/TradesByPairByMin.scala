//package org.esgi.project.domain.models
//
//import play.api.libs.json.{Json, OFormat}
//
//import java.time.{OffsetDateTime}
//
//case class TradesByPairByMin(eventTime: OffsetDateTime, pair: String, price: Double, quantity: Double) {
//  def apply(trade: Trade): TradesByPairByMin = {
//    TradesByPairByMin(trade.eventTime, trade.pair, trade.price, trade.quantity)
//  }
//}
//
//object TradesByPairByMin {
//  implicit val format: OFormat[Metric] = Json.format[Metric]
//}
