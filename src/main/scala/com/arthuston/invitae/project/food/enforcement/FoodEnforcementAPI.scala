/**
 * Food Enforcement API wrapper using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import spray.json.DefaultJsonProtocol.{jsonFormat2, jsonFormat5}
import spray.json.RootJsonFormat
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.javadsl.Source

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

//import MyJsonProtocol._
//import akka.http.scaladsl.common.EntityStreamingSupport
//import akka.http.scaladsl.common.JsonEntityStreamingSupport
//import MyJsonProtocol._
import akka.http.scaladsl.unmarshalling._
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.common.JsonEntityStreamingSupport

import spray.json.RootJsonFormat
import spray.json.DefaultJsonProtocol._

object FoodEnforcementAPI {
  implicit val system = ActorSystem(Behaviors.empty, "SingleRequest")
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.executionContext
  implicit val jsonStreamingSupport = EntityStreamingSupport.json()
  implicit val clientJsonFormat: RootJsonFormat[FoodEnforcementMetaResults] = jsonFormat5(FoodEnforcementMetaResults.apply)

  object FoodEnforcementDataJsonProtocol
    extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
      with spray.json.DefaultJsonProtocol {

  }


  private val UnmarshallDuration = Duration(1, TimeUnit.SECONDS)
  private val Endpoint = "https://api.fda.gov/food/enforcement.json"

  // unmarshal:
  def get(search: Option[String] = null, limit: Option[Int] = null): Unit = {
      // make asynch request
      val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = Endpoint))

      // handle response
      responseFuture
        .onComplete {
          case Success(response) =>
            val unmarshalled: Future[Source[FoodEnforcementMetaResults, NotUsed]] =
              Unmarshal(response).to[Source[FoodEnforcementMetaResults, NotUsed]]
            val foodEnforcementData = Await.result(unmarshalled, UnmarshallDuration) // don't block in non-test code!
            foodEnforcementData
          case Failure(_) => sys.error("something wrong")
        }
    }
}
//}
//
//object FoodEnforcementAPI {
//
//
//  def get(search: Option[String] = null, limit: Option[Int] = null): Unit = {
//    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = "http://akka.io"))
//    responseFuture
//      .onComplete {
//        case Success(res) => println(res)
//        case Failure(_) => sys.error("something wrong")
//      }
//    request
//    .
//    val isResponse = request.isResponse()
//    isResponse
//    //    // call API throw HTTPException if error
//    //    val response = requests.get(Endpoint)
//    //    checkResponse(response)
//    //
//    //    // convert bytes to JSON
//    //    val jsObject = Json.parse(response.bytes).as[JsObject]
//    //
//    //    // convert Json to FoodEnforcementMeta
//    //    implicit val foodEnforcementMetaResultsReads = Json.reads[FoodEnforcementMetaResults]
//    //    implicit val foodEnforcementMetaReads = Json.reads[FoodEnforcementMeta]
//    //
//    //    // convert Json to Seq[FoodEnforcementResult]
//    //    implicit val foodEnforcementResultsFormat = Json.format[FoodEnforcementResult]
//    //    implicit val foodEnforcementResultsRead = Reads.seq(foodEnforcementResultsFormat)
//    //
//    //    // convert Json to FoodEnforcementData
//    //    implicit val foodEnforcementDataReads = Json.reads[FoodEnforcementData]
//    //    val foodEnforcementDataFromJson: JsResult[FoodEnforcementData] =
//    //      Json.fromJson[FoodEnforcementData](jsObject)
//    //
//    //    foodEnforcementDataFromJson match {
//    //      case e@JsError(_) =>
//    //        throw new APIException.HTTPException(JsError.toJson(e).toString())
//    //    }
//    //
//    //    foodEnforcementDataFromJson.get
//  }
//  //
//  //  private def checkResponse(response: Response): Response = {
//  //    if (response.statusCode < 200 || response.statusCode > 299) {
//  //      throw new APIException.HTTPException(response.statusCode, response.statusMessage)
//  //    }
//  //    response
//  //  }
//  //
//  //
//}
