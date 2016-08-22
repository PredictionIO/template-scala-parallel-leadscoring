package org.template.leadscoring

import org.apache.predictionio.controller.PPreparator
//import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

class PreparedData(
  val labeledPoints: RDD[LabeledPoint],
  val featureIndex: Map[String, Int],
  val featureCategoricalIntMap: Map[String, Map[String, Int]]
) extends Serializable


class Preparator extends PPreparator[TrainingData, PreparedData] {

  @transient lazy val logger = Logger[this.type]

  private def createCategoricalIntMap(
    values: Array[String], // categorical values
    default: String // default cateegorical value
  ): Map[String, Int] = {
    val m = values.zipWithIndex.toMap
    if (m.contains(default))
      m
    else
      // add default value if origina values don't have it
      m + (default -> m.size)
  }

  def prepare(sc: SparkContext, td: TrainingData): PreparedData = {

    // find out all values of the each feature
    val landingValues = td.session.map(_.landingPageId).distinct.collect
    val referrerValues = td.session.map(_.referrerId).distinct.collect
    val browserValues = td.session.map(_.browser).distinct.collect

    // map feature value to integer for each categorical feature
    val featureCategoricalIntMap = Map(
      "landingPage" -> createCategoricalIntMap(landingValues, ""),
      "referrer" -> createCategoricalIntMap(referrerValues, ""),
      "browser" -> createCategoricalIntMap(browserValues, "")
    )
    // index position of each feature in the vector
    val featureIndex = Map(
      "landingPage" -> 0,
      "referrer" -> 1,
      "browser" -> 2
    )

    // inject some default to cover default cases
    val defaults = Seq(
      new Session(
        landingPageId = "",
        referrerId = "",
        browser = "",
        buy = false
      ),
      new Session(
        landingPageId = "",
        referrerId = "",
        browser = "",
        buy = true
      ))

    val defaultRDD = sc.parallelize(defaults)
    val sessionRDD = td.session.union(defaultRDD)

    val labeledPoints: RDD[LabeledPoint] = sessionRDD.map { session =>
      logger.debug(s"${session}")
      val label = if (session.buy) 1.0 else 0.0

      val feature = new Array[Double](featureIndex.size)
      feature(featureIndex("landingPage")) =
        featureCategoricalIntMap("landingPage")(session.landingPageId).toDouble
      feature(featureIndex("referrer")) =
        featureCategoricalIntMap("referrer")(session.referrerId).toDouble
      feature(featureIndex("browser")) =
        featureCategoricalIntMap("browser")(session.browser).toDouble

      LabeledPoint(label, Vectors.dense(feature))
    }.cache()

    logger.debug(s"labelelPoints count: ${labeledPoints.count()}")
    new PreparedData(
      labeledPoints = labeledPoints,
      featureIndex = featureIndex,
      featureCategoricalIntMap = featureCategoricalIntMap)
  }
}
