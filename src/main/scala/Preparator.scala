package org.template.leadscore

import io.prediction.controller.PPreparator
//import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

class PreparedData(
  val labeledPoints: RDD[LabeledPoint],
  val featureIndex: Map[String, Int],
  val categoricalFeatureMap: Map[String, Map[String, Int]]
) extends Serializable


class Preparator extends PPreparator[TrainingData, PreparedData] {

  @transient lazy val logger = Logger[this.type]

  private def categoricalMap(
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

    // create string to index BiMap
    val landValues = td.session.map(_.landId).distinct.collect
    val referralValues = td.session.map(_.referralId).distinct.collect
    val browserValues = td.session.map(_.browser).distinct.collect

    val categoricalFeatureMap = Map(
      "land" -> categoricalMap(landValues, ""),
      "referral" -> categoricalMap(referralValues, ""),
      "browser" -> categoricalMap(browserValues, "")
    )
    val featureIndex = Map(
      "land" -> 0,
      "referral" -> 1,
      "browser" -> 2
    )

    val defaults = Seq(
      new Session(
        landId = "",
        referralId = "",
        browser = "",
        buy = false
      ),
      new Session(
        landId = "",
        referralId = "",
        browser = "",
        buy = true
      ))



    val defaultRDD = sc.parallelize(defaults)
    val sessionRDD = td.session.union(defaultRDD)

    val labeledPoints: RDD[LabeledPoint] = sessionRDD.map { session =>
      logger.info(s"${session}")
      val label = if (session.buy) 1.0 else 0.0

      val feature = new Array[Double](featureIndex.size)
      feature(featureIndex("land")) =
        categoricalFeatureMap("land")(session.landId).toDouble
      feature(featureIndex("referral")) =
        categoricalFeatureMap("referral")(session.referralId).toDouble
      feature(featureIndex("browser")) =
        categoricalFeatureMap("browser")(session.browser).toDouble

      LabeledPoint(label, Vectors.dense(feature))
    }.cache()

    logger.info(s"labelelPoints count: ${labeledPoints.count()}")
    new PreparedData(
      labeledPoints = labeledPoints,
      featureIndex = featureIndex,
      categoricalFeatureMap = categoricalFeatureMap)
  }
}
