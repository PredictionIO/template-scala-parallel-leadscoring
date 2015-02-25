package org.template.leadscore

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  landId: String,
  referralId: String,
  browser: String
) extends Serializable

case class PredictedResult(
  score: Double
) extends Serializable

object LeadScoreEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("randomforest" -> classOf[RFAlgorithm]),
      classOf[Serving])
  }
}
