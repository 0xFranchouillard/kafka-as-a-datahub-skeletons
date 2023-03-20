package org.esgi.project.domain.models

import play.api.libs.json.{Json, OFormat}

case class MeanPriceByPairPerMin(sum: Double, count: Long, meanPrice: Double) {
  def increment(price: Double): MeanPriceByPairPerMin = this.copy(
    sum = this.sum + price,
    count =  this.count + 1
  ).computeMeanPrice

  private def computeMeanPrice: MeanPriceByPairPerMin = this.copy(
    meanPrice = this.sum / this.count
  )
}

object MeanPriceByPairPerMin {
  implicit val format: OFormat[MeanPriceByPairPerMin] = Json.format[MeanPriceByPairPerMin]

  def empty: MeanPriceByPairPerMin = MeanPriceByPairPerMin(0.0, 0, 0.0)
}