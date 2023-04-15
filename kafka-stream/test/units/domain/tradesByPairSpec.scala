package units.domain

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.domain.models.Trade
import org.esgi.project.domain.services.StreamProcessing
import org.scalatest.FunSuite

import java.lang
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

case class tradesByPairSpec(tradesByPairByMin: WindowStore[String, ValueAndTimestamp[Long]])
  extends FunSuite with PlayJsonSupport {
  def assertTradesByPairPerMin(trades: List[Trade]): Unit = {
    // Assert the count of trades by pair per minutes
    val tradesByPair: Map[String, Int] = trades
      .groupBy(_.pair)
      .map { case (pair, trade) => (pair, trade.size) }

    tradesByPair.foreach { case (pair, count) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Long]]] =
        this.tradesByPairByMin
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
        case Some(row) => assert(row.value.value() == count)
        case None => assert(false, s"No data for $pair in ${this.tradesByPairByMin.name()}")
      }
    }
  }
}

object tradesByPairSpec {
  def init(testDriver: TopologyTestDriver): tradesByPairSpec = {
    val tradesByPairByMin: WindowStore[String, ValueAndTimestamp[Long]] =
      testDriver.getTimestampedWindowStore[String, Long](StreamProcessing.tradesByPairByMinStoreName)
    tradesByPairSpec(tradesByPairByMin)
  }
}
