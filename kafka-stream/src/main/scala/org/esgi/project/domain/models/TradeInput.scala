package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}
import java.util.Date

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
