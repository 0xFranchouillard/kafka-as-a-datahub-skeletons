package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}

case class StockExchange(listPrices: List[Double], openingPrice: Double, endingPrice: Double, maxPrice: Double, minPrice: Double) {
  def increment(price: Double): StockExchange = {
    val prices = this.listPrices:::List(price)
    this.copy(
      listPrices = prices,
      openingPrice = prices.head,
      endingPrice = prices.last,
      maxPrice = prices.max,
      minPrice = prices.min
    )
  }
}


object StockExchange {
  implicit val format: OFormat[StockExchange] = Json.format[StockExchange]

  def empty: StockExchange = StockExchange(List(), 0.0, 0.0, 0.0, 0.0)
}
