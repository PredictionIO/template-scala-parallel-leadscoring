package org.template.leadscoring

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  landingPageId: String,
  referrerId: String,
  browser: String
) extends Serializable

case class PredictedResult(
  score: Double
) extends Serializable

object LeadScoringEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("randomforest" -> classOf[RFAlgorithm]),
      classOf[Serving])
  }
}
