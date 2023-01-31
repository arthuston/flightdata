/**
 * FlightDataAssignment flight data reader.
 * The flightData.csv file has the following columns:
 * Field        Description
 * passengerId  Integer representing the id of a passenger
 * flightId     Integer representing the id of a flight
 * From         String representing the departure country
 * To           String representing the destination country
 * Date         String representing the date of a flight
 */

package com.arthuston.flightdata

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object FlightConst {
  val FlightId = "flightId"
  val To = "to"
  val Date = "date"
  private val From = "from"
  val Schema = new StructType(Array(
    StructField(FlightAndPassengerConst.PassengerId, StringType),
    StructField(FlightId, StringType),
    StructField(From, StringType),
    StructField(To, StringType),
    StructField(Date, DateType),
  ))
  val File = "./input/FlightData.csv"
}
