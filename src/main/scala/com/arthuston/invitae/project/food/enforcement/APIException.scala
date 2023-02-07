/**
 * Simple HTTP Exception
 */
package com.arthuston.invitae.project.food.enforcement

import play.api.libs.json.JsResult
import requests.Response

object APIException {

  case class HTTPException(statusCode: Int, statusMessage: String) extends RuntimeException
  case class ParseException(parseMessage: String) extends RuntimeException

  def checkResponse(response: Response): Response = {
    if (response.statusCode < 200 || response.statusCode > 299) {
      throw HTTPException(response.statusCode, response.statusMessage)
    }
    response
  }

  def checkJsResult(jsResult: JsResult[Any]): JsResult[Any] = {
    if (jsResult.isError) {
      throw ParseException("Error parsing %s".format(jsResult.toString()))
    }
    return jsResult
  }
}
