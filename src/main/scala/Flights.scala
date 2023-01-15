/**
 * Quantexa FlightDataAssignment flight data reader.
 * The flightData.csv file has the following columns:
 * Field        Description
 * passengerId  Integer representing the id of a passenger
 * flightId     Integer representing the id of a flight
 * From         String representing the departure country
 * To           String representing the destination country
 * Date         String representing the date of a flight
 */

package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, DateType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Flights {
  // schema
  val PassengerId = "passengerId"
  val FlightId = "flightId"
  val From = "from"
  val To = "to"
  val Date = "date"
  val Schema = StructType(Array(
    StructField(PassengerId, StringType, true),
    StructField(FlightId, StringType, true),
    StructField(From, StringType, true),
    StructField(To, StringType, true),
    StructField(Date, DateType, true))
  )
}
class Flights(spark: SparkSession, path: String) {
  val df = spark.read.option("header", true).schema(Flights.Schema).csv(path)

  // return DataFrame
  def data(): DataFrame = {
    df
  }
}
