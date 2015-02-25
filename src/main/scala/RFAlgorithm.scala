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
  val featureIndex: Map[String, Int],
  val categoricalFeatureMap: Map[String, Map[String, Int]]
) extends Serializable {
  override def toString = {
    s" forest: [${forest}]" +
    s" featureIndex: ${featureIndex}" +
    s" categoricalFeatureMap: ${categoricalFeatureMap}"
  }
}

// extends P2LAlgorithm because the MLlib's RandomForestModel doesn't
// contain RDD.
class RFAlgorithm(val ap: RFAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RFModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(pd: PreparedData): RFModel = {

    val categoricalFeaturesInfo = pd.categoricalFeatureMap.map { case (f, m) =>
      (pd.featureIndex(f), m.size)
    }

    logger.info(s"${categoricalFeaturesInfo}")

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
      featureIndex = pd.featureIndex,
      categoricalFeatureMap = pd.categoricalFeatureMap
    )
  }

  def predict(model: RFModel, query: Query): PredictedResult = {

    val featureIndex = model.featureIndex
    val categoricalFeatureMap = model.categoricalFeatureMap

    val landFeature = categoricalFeatureMap("land").get(query.landId)
      .map(_.toDouble).getOrElse{
        logger.info(s"Not found ${query.landId} in categoricalFeatureMap")
        categoricalFeatureMap("land")("").toDouble
      }
    val referralFeature = categoricalFeatureMap("referral")
      .get(query.referralId).map(_.toDouble).getOrElse{
        logger.info(s"Not found ${query.referralId} in categoricalFeatureMap")
        categoricalFeatureMap("referral")("").toDouble
      }
    val browserFeature = categoricalFeatureMap("browser")
      .get(query.browser).map(_.toDouble).getOrElse{
        logger.info(s"Not found ${query.browser} in categoricalFeatureMap")
        categoricalFeatureMap("browser")("").toDouble
      }
    // reate feature Array
    val feature = new Array[Double](model.featureIndex.size)
    feature(featureIndex("land")) = landFeature
      //categoricalFeatureMap("land").get(query.landId).toDouble
    feature(featureIndex("referral")) = referralFeature
      //categoricalFeatureMap("referral")(query.referralId).toDouble
    feature(featureIndex("browser")) = browserFeature
      //categoricalFeatureMap("browser")(query.browser).toDouble

    val score = model.forest.predict(Vectors.dense(feature))
    new PredictedResult(score)
  }

}
