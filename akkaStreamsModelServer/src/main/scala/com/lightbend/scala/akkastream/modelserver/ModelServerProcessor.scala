package com.lightbend.scala.akkastream.modelserver

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Graph, SourceShape}
import akka.util.Timeout
import com.lightbend.model.winerecord.WineRecord
import com.lightbend.scala.akkastream.modelserver.actors.ModelServingManager
import com.lightbend.scala.akkastream.modelserver.stage.{ModelStateStore, ModelStage}
import com.lightbend.scala.akkastream.queryablestate.actors.RestServiceActors
import com.lightbend.scala.akkastream.queryablestate.inmemory.RestServiceInMemory
import com.lightbend.scala.modelServer.model.{ModelWithDescriptor, ServingResult}

import scala.concurrent.duration._

/**
  * Abstraction for the core logic for model serving.
  */
trait ModelServerProcessor {
  def createStreams(dataStream: Source[WineRecord, Consumer.Control], modelStream: Source[ModelWithDescriptor, Consumer.Control])
                   (implicit system: ActorSystem, materializer: ActorMaterializer): Unit
}

/**
  * Implements model serving using an Actor-based approach, to which messages are sent to do scoring.
  */
object ActorModelServerProcessor extends ModelServerProcessor {

  def createStreams(dataStream: Source[WineRecord, Consumer.Control], modelStream: Source[ModelWithDescriptor, Consumer.Control])
                   (implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {

    println("*** Using an Actor-based model server implementation ***")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = system.actorOf(ModelServingManager.props)

    // Model stream processing
    modelStream
      .ask[String](1)(modelserver)
      // Another way to invoke serving asynchronously (previous line), but less optimal)
      // .mapAsync(1)(elem => modelserver ? elem)
      .runWith(Sink.ignore) // run the stream, we do not read the results directly

    // Data stream processing
    dataStream
      .ask[ServingResult](1)(modelserver)
      // Another way to invoke serving asynchronously (previous line), but less optimal)
      // .mapAsync(1)(elem => (modelserver ? elem).mapTo[ServingResult])
      .runForeach(result => {
      if (result.processed) {
        println(s"Calculated quality - ${result.result} calculated in ${result.duration} ms")
      } else {
        println("No model available - skipping")
      }
    })
    // Exercise:
    // We just used `runForeach`, which iterates through the records, prints output, but doesn't
    // return a value. (In functional programming terms, it's "pure side effects")
    // In particular, we might want to write the results to a new Kafka topic.
    // 1. Modify the "client" project to create a new output topic. (Or you could do it here.)
    // 2. Modify AkkaModelServer to add the configuration for the new topic. For example, copy and adapt
    //    `dataConsumerSettings` for a new producer instead of a consumer.
    // 3. Replace `runForeach` with logic to write the results to the new Kafka topic.
    //    Also keep the current `println` output for convenience. For information on writing to Kafka
    //    from Akka Streams, see:
    //    https://doc.akka.io/docs/akka-stream-kafka/current/producer.html#producer-as-a-sink

    // Exercise:
    // Repeat the previous exercise, but write the results to the local file system instead (easier).

    // Rest Server
    RestServiceActors.startRest(modelserver)
  }
}

/**
  * Implements model serving using a custom Akka Streams "stage", so that scoring looks like a regular stream "operator".
  */
object CustomStageModelServerProcessor extends ModelServerProcessor {

  def createStreams(dataStream: Source[WineRecord, Consumer.Control], modelStream: Source[ModelWithDescriptor, Consumer.Control])
                   (implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {

    println("*** Using the Custom Stage model server implementation ***")

    val logged = Flow[ServingResult].map { result =>
      if (result.processed) {
        println(s"Calculated quality - ${result.result} calculated in ${result.duration} ms");
        Some(result.result)
      } else {
        println("No model available - skipping")
        None
      }
    }


    val predictor = new ModelStage()

    val graph: Graph[SourceShape[ServingResult], ModelStateStore] = GraphDSL.create(predictor) { implicit b =>
      s =>
        import GraphDSL.Implicits._

        dataStream ~> s.in0
        modelStream ~> s.in1

        SourceShape(s.out)
    }

    val predictions: Source[ServingResult, ModelStateStore] = Source.fromGraph(graph)

    val modelStateStore: ModelStateStore = predictions.via(logged).to(Sink.ignore).run()

    RestServiceInMemory.startRest(modelStateStore)
  }
}
