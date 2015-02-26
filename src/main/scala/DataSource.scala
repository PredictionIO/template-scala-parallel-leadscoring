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
        // the first view event of the session is the landing event
        val landing = viewIter.reduce{ (a, b) =>
          if (a.eventTime.isBefore(b.eventTime)) a else b
        }
        // any buy after landing
        val buy = buyIter.filter( b => b.eventTime.isAfter(landing.eventTime))
          .nonEmpty

        try {
          new Session(
            landingPageId = landing.targetEntityId.get,
            referrerId = landing.properties.getOrElse[String]("referrerId", ""),
            browser = landing.properties.getOrElse[String]("browser", ""),
            buy = buy
          )
        } catch {
          case e: Exception => {
            logger.error(s"Cannot create session data from ${landing}. ${e}.")
            throw e
          }
        }
      }.cache()

    new TrainingData(session)
  }
}


case class Session(
  landingPageId: String,
  referrerId: String,
  browser: String,
  buy: Boolean // buy or not
) extends Serializable

class TrainingData(
  val session: RDD[Session]
) extends Serializable
