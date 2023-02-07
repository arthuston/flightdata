/**
 * Food Enforcement API wrapper using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import play.api.libs.json.{JsResult, JsValue, Json}
import requests.Response

import scala.collection.mutable.ListBuffer
import com.arthuston.invitae.project.food.enforcement.FoodEnforcementDataImplicits

object FoodEnforcementAPI {
  val MaxLimit: Option[Int] = Option[Int](1000)
  private val Endpoint = "https://api.fda.gov/food/enforcement.json"

  // unmarshal:
  def get(search: Option[String] = None, limit: Option[Int] = None): FoodEnforcementData = {
    // handle options
    var optionsBuffer = ListBuffer[String]()
    if (search.isDefined) {
      optionsBuffer += "search=%s".format(search.get)
    }
    if (limit.isDefined) {
      optionsBuffer += "limit=%d".format(limit.get)
    }
    val optionsList = optionsBuffer.toList
    val optionsString = if (optionsList.isEmpty) "" else "?%s".format(optionsList.mkString("&"))
    val url: String = ("%s%s".format(Endpoint, optionsString))

    // call endpoint using https://github.com/com-lihaoyi/requests-scala
    // other options: akka or play
    val response: Response = requests.get(url)

    // check http status
    APIException.checkResponse(response)

    // get json as string
    val jsonString = new String(response.bytes)

    // use play json https://www.playframework.com/documentation/2.0/ScalaJson
    // to convert. Other options: spray-json https://github.com/spray/spray-json
    val jsValue: JsValue = Json.parse(jsonString)
    val foodEnforcementDataJsResult: JsResult[FoodEnforcementData] = FoodEnforcementDataImplicits.foodEnforcementDataReads.reads(jsValue)

    // check parse error
    APIException.checkJsResult(foodEnforcementDataJsResult)
    foodEnforcementDataJsResult.get
  }
}
