package com.lightbend.scala.akkastream.modelserver.stage

import java.util.concurrent.TimeUnit

import akka.stream._
import akka.stream.stage._
import com.lightbend.model.winerecord.WineRecord
import com.lightbend.scala.modelServer.model.{Model, ModelToServeStats, ModelWithDescriptor, ServingResult}

class ModelStage extends GraphStageWithMaterializedValue[FanInShape2[WineRecord, ModelWithDescriptor, ServingResult], ModelStateStore] {

  override val shape: FanInShape2[WineRecord, ModelWithDescriptor, ServingResult] = new FanInShape2("other_model_shape")

  class ModelLogic extends GraphStageLogicWithLogging(shape) {
    // state must be kept in the Logic instance, since it is created per stream materialization
    private var currentModel: Option[Model] = None
    var currentState: Option[ModelToServeStats] = None
    var delayedRecord: Option[WineRecord] = None

    private def scoreWine(record: WineRecord, model: Model): Unit = {
      val start = System.nanoTime()
      val quality = model.score(record).asInstanceOf[Double]
      val duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
      currentState = currentState.map(_.incrementUsage(duration))
      push(shape.out, ServingResult(quality, duration))
    }

    setHandler(shape.in0, new InHandler {
      override def onPush(): Unit = {

        val record = grab(shape.in0)

        currentModel match {
          case Some(model) =>
            scoreWine(record, model)
          case None =>
            // No model yet, statsh the record and request a model.
            delayedRecord = Some(record)
            pull(shape.in1)
        }
      }
    })

    setHandler(shape.in1, new InHandler {
      override def onPush(): Unit = {
        val modelWithDescriptor = grab(shape.in1)
        println(s"Set model [$modelWithDescriptor], requesting more model.")
        currentModel = Some(modelWithDescriptor.model)
        currentState = Some(ModelToServeStats(modelWithDescriptor.descriptor))
        pull(shape.in1)
        delayedRecord.foreach { record =>
          scoreWine(record, modelWithDescriptor.model)
          delayedRecord = None
        }
      }
    })

    setHandler(shape.out, new OutHandler {
      override def onPull(): Unit = {
        pull(shape.in0)
      }
    })

  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, ModelStateStore) = {
    val logic = new ModelLogic

    // we materialize this value so whoever runs the stream can get the current serving info
    val modelStateStore = new ModelStateStore {
      override def getCurrentServingInfo: ModelToServeStats =
        logic.currentState.getOrElse(ModelToServeStats.empty)

      override def setModel(model: ModelWithDescriptor): Unit = () // Ignore writes. Model updated through inlet.
    }
    (logic, modelStateStore)
  }

}

