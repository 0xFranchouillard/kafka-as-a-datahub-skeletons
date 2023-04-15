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

case class meanPriceByPairSpec(meanPriceByPairPerMin:
                                     WindowStore[String, ValueAndTimestamp[MeanPriceByPairPerMin]])
  extends FunSuite with PlayJsonSupport {
  def assertMeanPriceByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the mean price by pair per minutes
    val meanPriceByPair: Map[String, Double] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) =>
        val meanPrice = trade.map(_.price).sum / trade.size
        (pair, meanPrice)
      }

    meanPriceByPair.foreach { case (pair, meanPrice) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[MeanPriceByPairPerMin]]] =
        this.meanPriceByPairPerMin
          .fetch(
            pair,
            OffsetDateTime.ofInstant(trades.head.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant,
            OffsetDateTime.ofInstant(trades.last.eventTime.toInstant, ZoneOffset.UTC)
              .truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().meanPrice == meanPrice)
        case None => assert(false, s"No data for $pair in ${this.meanPriceByPairPerMin.name()}")
      }
    }
  }
}

object meanPriceByPairSpec {
  def init(testDriver: TopologyTestDriver): meanPriceByPairSpec = {
    val meanPriceByPairPerMin: WindowStore[String, ValueAndTimestamp[MeanPriceByPairPerMin]] =
      testDriver.getTimestampedWindowStore[String, MeanPriceByPairPerMin](
        StreamProcessing.meanPriceByPairPerMinStoreName
      )
    meanPriceByPairSpec(meanPriceByPairPerMin)
  }
}