package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}

case class stockExchange(listPrices: List[Double], openingPrice: Double, endingPrice: Double, maxPrice: Double, minPrice: Double) {
  def increment(price: Double): stockExchange = this.copy(
    listPrices = this.listPrices:::List(price),
    openingPrice = listPrices.head,
    endingPrice = listPrices.last,
    maxPrice = listPrices.max,
    minPrice = listPrices.min
  )
}


object stockExchange {
  implicit val format: OFormat[stockExchange] = Json.format[stockExchange]

  def empty: stockExchange = stockExchange(List(), 0.0, 0.0, 0.0, 0.0)
}
