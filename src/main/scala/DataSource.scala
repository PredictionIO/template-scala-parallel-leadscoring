package org.template.leadscore

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()

    val viewPage: RDD[(String, Event)] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(Seq("view")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("page")))(sc)
      // eventsDb.find() returns RDD[Event]
      .map { event =>
        val sessionId = try {
          event.properties.get[String]("sessionId")
        } catch {
          case e: Exception => {
            logger.error(s"Cannot get sessionId from event ${event}. ${e}.")
            throw e
          }
        }
        (sessionId, event)
      }

    val buyItem: RDD[(String, Event)] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(Seq("buy")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      // eventsDb.find() returns RDD[Event]
      .map { event =>
        val sessionId = try {
          event.properties.get[String]("sessionId")
        } catch {
          case e: Exception => {
            logger.error(s"Cannot get sessionId from event ${event}. ${e}.")
            throw e
          }
        }
        (sessionId, event)
      }

    val session: RDD[Session] = viewPage.cogroup(buyItem)
      .map { case (sessionId, (viewIter, buyIter)) =>
        //
        val land = viewIter.reduce{ (a, b) =>
          if (a.eventTime.isBefore(b.eventTime)) a else b
        }
        // any buy after land
        val buy = buyIter.filter( b => b.eventTime.isAfter(land.eventTime))
          .nonEmpty

        try {
          new Session(
            landId = land.targetEntityId.get,
            referralId = land.properties.getOrElse[String]("referralId", ""),
            browser = land.properties.getOrElse[String]("browser", ""),
            buy = buy
          )
        } catch {
          case e: Exception => {
            logger.error(s"Cannot create session data from ${land}. ${e}.")
            throw e
          }
        }
      }.cache()

    new TrainingData(session)
  }
}


case class Session(
  landId: String, // landing page ID
  referralId: String, // referral page ID
  browser: String, // browser
  buy: Boolean // buy or not
) extends Serializable

class TrainingData(
  val session: RDD[Session]
) extends Serializable
