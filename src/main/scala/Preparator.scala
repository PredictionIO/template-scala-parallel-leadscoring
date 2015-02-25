package org.template.leadscore

import io.prediction.controller.PPreparator
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

class PreparedData(
  val labeledPoints: RDD[LabeledPoint],
  val landFeatureMap: BiMap[String, Int],
  val referralFeatureMap: BiMap[String, Int],
  val browserFeatureMap: BiMap[String, Int]
) extends Serializable

class Preparator extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, td: TrainingData): PreparedData = {

    // create string to index BiMap
    val landFeatureMap = BiMap.stringInt(td.session.map(_.landId))
    val referralFeatureMap = BiMap.stringInt(td.session.map(_.referralId))
    val browserFeatureMap = BiMap.stringInt(td.session.map(_.browser))

    val labeledPoints: RDD[LabeledPoint] = td.session.map { session =>
      val label = if (session.buy) 1.0 else 0.0
      val landFeature = landFeatureMap(session.landId).toDouble
      val referralFeature = referralFeatureMap(session.referralId).toDouble
      val browserFeature = browserFeatureMap(session.browser).toDouble

      val feature = Array(
        landFeature,
        referralFeature,
        browserFeature)

      LabeledPoint(label, Vectors.dense(feature))
    }.cache()

    new PreparedData(
      labeledPoints = labeledPoints,
      landFeatureMap = landFeatureMap,
      referralFeatureMap = referralFeatureMap,
      browserFeatureMap = browserFeatureMap)
  }
}
