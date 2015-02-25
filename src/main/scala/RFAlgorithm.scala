package org.template.leadscore

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class RFAlgorithmParams(
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int
) extends Params

class RFModel(
  val forest: RandomForestModel,
  val landFeatureMap: BiMap[String, Int],
  val referralFeatureMap: BiMap[String, Int],
  val browserFeatureMap: BiMap[String, Int]
) extends Serializable

// extends P2LAlgorithm because the MLlib's RandomForestModel doesn't
// contain RDD.
class RFAlgorithm(val ap: RFAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RFModel, Query, PredictedResult] {

  def train(pd: PreparedData): RFModel = {

    val categoricalFeaturesInfo = Map(
      0 -> pd.landFeatureMap.size,
      1 -> pd.referralFeatureMap.size,
      2 -> pd.browserFeatureMap.size
    )

    val forestModel: RandomForestModel = RandomForest.trainRegressor(
      pd.labeledPoints,
      categoricalFeaturesInfo,
      ap.numTrees,
      ap.featureSubsetStrategy,
      ap.impurity,
      ap.maxDepth,
      ap.maxBins)

    new RFModel(
      forest = forestModel,
      landFeatureMap = pd.landFeatureMap,
      referralFeatureMap = pd.referralFeatureMap,
      browserFeatureMap = pd.browserFeatureMap
    )
  }

  def predict(model: RFModel, query: Query): PredictedResult = {

    val landFeature = model.landFeatureMap(query.landId).toDouble
    val referralFeature = model.referralFeatureMap(query.referralId).toDouble
    val browserFeature = model.browserFeatureMap(query.browser).toDouble

    val feature = Array(
      landFeature,
      referralFeature,
      browserFeature)

    val score = model.forest.predict(Vectors.dense(feature))
    new PredictedResult(score)
  }

}
