package org.esgi.project.api

import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.esgi.project.domain.services.StreamProcessing
import org.esgi.project.api.models._
import org.esgi.project.domain.models.{MeanPriceByPairPerMin, StockExchange}

import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.IteratorHasAsScala



class API(streamApp: KafkaStreams) {

  def getTradesStats(from: OffsetDateTime, to:OffsetDateTime, targetPair: String): StatsByPairByHour = {
    val timeFrom = from.toInstant
    val timeTo = to.toInstant

    val tradesByPairByMin: ReadOnlyWindowStore[String, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.tradesByPairByMinStoreName,
        QueryableStoreTypes.windowStore[String, Long]()
      )
    )

    val tradesOverLastHour = tradesByPairByMin.fetchAll(timeFrom, timeTo).asScala.toList
      .groupBy(_.key.key())
      .filter {
        case (pair, _) => pair == targetPair
      }.map {
        case (_, windowedTradeList) =>
          windowedTradeList.map(_.value).sum
      }.toList.sum

    val meanPriceByPairPerMin: ReadOnlyWindowStore[String, MeanPriceByPairPerMin] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.meanPriceByPairPerMinStoreName,
        QueryableStoreTypes.windowStore[String, MeanPriceByPairPerMin]()
      )
    )

    val averagePriceOverLastHour: Double = meanPriceByPairPerMin.fetchAll(timeFrom, timeTo).asScala.toList
      .groupBy(_.key.key())
      .filter {
        case (pair, _) => pair == targetPair
      }.map {
      case (_, windowedMeanPriceList) =>
          windowedMeanPriceList.map(_.value.meanPrice).sum
      }.toList.sum / tradesOverLastHour

    val tradesVolByPairPerHour: ReadOnlyWindowStore[String, Double] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.tradesVolByPairPerHourStoreName,
        QueryableStoreTypes.windowStore[String, Double]()
      )
    )

    val volumeOverLastHourTest: Double = tradesVolByPairPerHour.fetchAll(timeFrom, timeTo).asScala.toList
      .groupBy(_.key.key())
      .filter {
        case (pair, _) => pair == targetPair
      }.map {
      case (_, windowedTradeList) =>
        windowedTradeList.map(_.value).sum
    }.toList.sum

//    val windows = tradesVolByPairPerHour.fetchAll(timeFrom, timeTo).asScala.toList
//    val groupedWindows = windows.groupBy(window => window.key.window().startTime().atOffset(ZoneOffset.UTC))
//    val volumeOverLastHour = groupedWindows.map {
//      case (timestamp, windowList) =>
//        val tradeList = windowList.map(window => tradeForPair(window.key.key(), window.value))
//        tradeForPairInWindow(timestamp, tradeList)
//    }.toList
//    tradeForPairLastHourResponse(volumeOverLastHour.filter(_.trades.contains(targetPair)))

    val statsByPairByHour = StatsByPairByHour(
      pair = targetPair,
      trades_over_last_hour = tradesOverLastHour,
      volume_over_last_hour = volumeOverLastHourTest,
      average_price_over_last_hour = averagePriceOverLastHour
    )

    statsByPairByHour
  }

  def getTradesCandles(from: OffsetDateTime, to:OffsetDateTime, targetPair: String): CandlesticksResponseList = {
    val timeFrom = from.toInstant
    val timeTo = to.toInstant

    val stockExchangeByPairPerMinStore: ReadOnlyWindowStore[String, StockExchange] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.stockExchangeByPairPerMin,
        QueryableStoreTypes.windowStore[String,StockExchange]()
      )
    )

    val stockExchangeByPairPerMinResponse: List[StockExchangeByPairInWindow] = stockExchangeByPairPerMinStore
      .fetchAll(timeFrom, timeTo).asScala.toList
      .groupBy(_.key.window().startTime())
      .map { case (timestamp, windowedStockExchangeList) =>
        new StockExchangeByPairInWindow(timestamp = timestamp.atOffset(ZoneOffset.UTC),
          stocksExchange = windowedStockExchangeList.map(
            stockExchangeByPair => new StockExchangeByPair(pair = stockExchangeByPair.key.key(),
              stockExchange =  stockExchangeByPair.value)
          )
        )
      }.toList.filter(_.stocksExchange.contains(targetPair))

    val tradesVolByPairPerMinStore: ReadOnlyWindowStore[String, Double] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.tradesVolByPairPerMinStoreName,
        QueryableStoreTypes.windowStore[String, Double]()
      )
    )

    val tradesVolByPairPerMinResponse: List[VolumeByPairInWindow] = tradesVolByPairPerMinStore
      .fetchAll(timeFrom, timeTo).asScala.toList
      .groupBy(_.key.window().startTime())
      .map { case (timestamp, windowedTradeList) =>
        new VolumeByPairInWindow(timestamp = timestamp.atOffset(ZoneOffset.UTC),
          volumes = windowedTradeList.map(
            tradesVolByPair => new VolumeByPair(pair = tradesVolByPair.key.key(), volume = tradesVolByPair.value)
          )
        )
      }.toList.filter(_.volumes.contains(targetPair))

    val mergedList = stockExchangeByPairPerMinResponse
      .collect {
        case stockExchangeByPairInWindow
          if tradesVolByPairPerMinResponse.exists(_.timestamp == stockExchangeByPairInWindow.timestamp) =>
          (stockExchangeByPairInWindow.timestamp,
            stockExchangeByPairInWindow.stocksExchange.last,
            tradesVolByPairPerMinResponse.find(_.timestamp == stockExchangeByPairInWindow.timestamp).get.volumes.last)
      }.map { case (date, stockExchange, tradesVol) => CandlesticksResponse(
      openingPrice = stockExchange.stockExchange.openingPrice,
      closingPrice = stockExchange.stockExchange.endingPrice,
      lowestPrice = stockExchange.stockExchange.minPrice,
      highestPrice = stockExchange.stockExchange.maxPrice,
      volume = tradesVol.volume,
      timestampValue = date)
    }

    CandlesticksResponseList(pair = targetPair, value = mergedList)
  }
}
