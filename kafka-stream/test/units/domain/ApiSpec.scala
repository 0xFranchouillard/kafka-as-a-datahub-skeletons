package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.domain.models.{MeanPriceByPairPerMin, Trade}
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest.FunSuite

import java.lang
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

case class EndPointSpec(path):
  extends FunSuite with PlayJsonSupport {
  //GET /trades/:pair/candles?from=??&to=??
  def EndPointTesting(path): Unit = {
      request(path) match {
        case Some(response) => assert(response.status == 200)
        status = response.status
        case None => assert(false, s"$status No data for $pair in ${this.meanPriceByPairPerMin.name()}")
      }
  }
  }
}
object EndPointSpec {
  def init(testDriver: TopologyTestDriver): EndPointSpec = {
    path = "/trades/BTCUSDT/"
    EndPointSpec(path)
  }
}