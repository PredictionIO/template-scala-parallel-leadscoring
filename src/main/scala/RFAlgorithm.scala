package org.template.leadscoring

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class RFAlgorithmParams(
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int,
  seed: Option[Int]
) extends Params

class RFModel(
  val forest: RandomForestModel,
  val featureIndex: Map[String, Int],
  val featureCategoricalIntMap: Map[String, Map[String, Int]]
) extends Serializable {
  override def toString = {
    s" forest: [${forest}]" +
    s" featureIndex: ${featureIndex}" +
    s" featureCategoricalIntMap: ${featureCategoricalIntMap}"
  }
}

// extends P2LAlgorithm because the MLlib's RandomForestModel doesn't
// contain RDD.
class RFAlgorithm(val ap: RFAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RFModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, pd: PreparedData): RFModel = {

    val categoricalFeaturesInfo = pd.featureCategoricalIntMap
      .map { case (f, m) =>
        (pd.featureIndex(f), m.size)
      }

    logger.info(s"categoricalFeaturesInfo: ${categoricalFeaturesInfo}")

    // use random seed if seed is not specified
    val seed = ap.seed.getOrElse(scala.util.Random.nextInt())

    val forestModel: RandomForestModel = RandomForest.trainRegressor(
      input = pd.labeledPoints,
      categoricalFeaturesInfo = categoricalFeaturesInfo,
      numTrees = ap.numTrees,
      featureSubsetStrategy = ap.featureSubsetStrategy,
      impurity = ap.impurity,
      maxDepth = ap.maxDepth,
      maxBins = ap.maxBins,
      seed = seed)

    new RFModel(
      forest = forestModel,
      featureIndex = pd.featureIndex,
      featureCategoricalIntMap = pd.featureCategoricalIntMap
    )
  }

  def predict(model: RFModel, query: Query): PredictedResult = {

    val featureIndex = model.featureIndex
    val featureCategoricalIntMap = model.featureCategoricalIntMap

    val landingPageId = query.landingPageId
    val referrerId = query.referrerId
    val browser = query.browser

    // look up categorical feature Int for landingPageId
    val landingFeature = lookupCategoricalInt(
      featureCategoricalIntMap = featureCategoricalIntMap,
      feature = "landingPage",
      value = landingPageId,
      default = ""
    ).toDouble


    // look up categorical feature Int for referrerId
    val referrerFeature = lookupCategoricalInt(
      featureCategoricalIntMap = featureCategoricalIntMap,
      feature = "referrer",
      value = referrerId,
      default = ""
    ).toDouble

    // look up categorical feature Int for brwoser
    val browserFeature = lookupCategoricalInt(
      featureCategoricalIntMap = featureCategoricalIntMap,
      feature = "browser",
      value = browser,
      default = ""
    ).toDouble

    // create feature Array
    val feature = new Array[Double](model.featureIndex.size)
    feature(featureIndex("landingPage")) = landingFeature
    feature(featureIndex("referrer")) = referrerFeature
    feature(featureIndex("browser")) = browserFeature

    val score = model.forest.predict(Vectors.dense(feature))
    new PredictedResult(score)
  }

  private def lookupCategoricalInt(
    featureCategoricalIntMap: Map[String, Map[String, Int]],
    feature: String,
    value: String,
    default: String): Int = {

    featureCategoricalIntMap(feature)
      .get(value)
      .getOrElse {
        logger.info(s"Unknown ${feature} ${value}." +
          " Default feature value will be used.")
        // use default feature value
        featureCategoricalIntMap(feature)(default)
      }
  }

}
