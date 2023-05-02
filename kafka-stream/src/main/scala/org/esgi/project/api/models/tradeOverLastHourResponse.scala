package org.esgi.project.api.models
import play.api.libs.json.{Json, OFormat}

import java.time.OffsetDateTime

case class StatsByPairByHour(
                              pair: String,
                              trades_over_last_hour: Long,
                              volume_over_last_hour: Double,
                              average_price_over_last_hour: Double
                            )

object StatsByPairByHour {
  implicit val format: OFormat[StatsByPairByHour] = Json.format[StatsByPairByHour]
}

case class tradeForPair(
                         pair: String,
                         trades_over_last_hour: Long)

object tradeForPair {
  implicit val format: OFormat[tradeForPair] = Json.format[tradeForPair]
}

case class tradeForPairInWindow(
                                 timestamp: OffsetDateTime,
                                 trades: List[tradeForPair]
                               )

//case class tradeForPairLastHourResponse(value: List[tradeForPairInWindow])
//object tradeForPairLastHourResponse {
//  implicit val format: OFormat[tradeForPairLastHourResponse] = Json.format[tradeForPairLastHourResponse]
//}
//case class tradeOverLastHourResponse(
//  pair: String,
//  trades_over_last_hour: Long,
//
