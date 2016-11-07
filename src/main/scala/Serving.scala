package org.template.leadscoring

import org.apache.predictionio.controller.LServing

import grizzled.slf4j.Logger

class Serving extends LServing[Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def serve(query: Query,
    predictedResults: Seq[PredictedResult]): PredictedResult = {
    predictedResults.head
  }
}
