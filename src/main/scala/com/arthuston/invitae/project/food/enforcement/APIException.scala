/**
 * Simple HTTP Exception
 */
package com.arthuston.invitae.project.food.enforcement
object APIException {

  class HTTPException(statusCode: Int, statusMessage: String) extends RuntimeException
  class ParseException(parseMessage: String) extends RuntimeException
}
