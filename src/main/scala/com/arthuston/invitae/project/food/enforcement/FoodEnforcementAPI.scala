/**
 * Food Enforcement API wrapper using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import play.api.libs.json.{JsResult, JsValue, Json}
import requests.Response

import scala.collection.mutable.ListBuffer

object FoodEnforcementAPI {
  private val MaxLimit: Option[Int] = Option[Int](1000)
  private val Endpoint = "https://api.fda.gov/food/enforcement.json"

  /**
   * Get FoodEnforcementData data (meta and results)
   * This offers partial support for the query parameters in the API;
   * a better implementation would offer full support for the query parameters.
   *
   * @param search search field:term
   * @param limit maximum number of records to return (max 1000)
   * @param skip number of records to skip
   * @return FoodEnforcementData data (meta and results)
   */
  private def getOnce(search: Option[String] = None, limit: Option[Int] = None, skip: Option[Int] = None): FoodEnforcementData = {
    // handle options
    val optionsBuffer = ListBuffer[String]()
    if (search.isDefined) {
      optionsBuffer += "search=%s".format(search.get)
    }
    if (limit.isDefined) {
      optionsBuffer += "limit=%d".format(limit.get)
    }
    if (skip.isDefined) {
      optionsBuffer += "skip=%d".format(skip.get)
    }
    val optionsList = optionsBuffer.toList
    val optionsString = if (optionsList.isEmpty) "" else "?%s".format(optionsList.mkString("&"))
    val url: String = "%s%s".format(Endpoint, optionsString)

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

  /**
   * Get all FoodEnforcementData data (last meta and results)
   * by combining multiple API calls until all data is returned.
   * This offers partial support for the query parameters in the API;
   * a better implementation would offer full support for the query parameters.
   *
   * @param search search field:term
   * @return FoodEnforcementData (last meta, all records)
   */
  def get(search: Option[String] = None): FoodEnforcementData = {
    // get first
    val firstData = getOnce(search = search, limit = MaxLimit)
    var meta = firstData.meta
    var results = firstData.results

    // get next until we have all data
    while (results.length < meta.results.total) {
      val nextData = getOnce(search = search, limit = MaxLimit, skip = Option[Int](results.length))
      meta = nextData.meta
      results = results ++ nextData.results
    }

    // return meta and results
    FoodEnforcementData(meta=meta, results=results)
  }
}
