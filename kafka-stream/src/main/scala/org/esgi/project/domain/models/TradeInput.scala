package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}
import java.util.Date

case class TradeInput(
    e: String,
    E: Date,
    s: String,
    t: Long,
    p: String,
    q: String,
    b: Long,
    a: Long,
    T: Date,
    m: Boolean,
    M: Boolean
)

object TradeInput {
  implicit val format: OFormat[TradeInput] = Json.format[TradeInput]
}
