/**
 * Quantexa FlightDataAssignment flight data reader.
 * The passengers.csv file has the following columns:
 * Field        Description
 * passengerId  Integer representing the id of a passenger
 * firstName    String representing the first name of a passenger
 * lastName     String representing the last name of a passenger
 */

package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Passengers {
  val PassengerId = "passengerId"
  val FirstName = "firstName"
  val LastName = "lastName"
  val Schema = StructType(Array(
    StructField(PassengerId, StringType, true),
    StructField(FirstName, StringType, true),
    StructField(LastName, StringType, true))
  )
}

class Passengers(spark: SparkSession, path: String) {
  var df = spark.read.option("header", true).schema(Passengers.Schema).csv(path)

  // return DataFrame
  def data(): DataFrame = {
    df
  }
}
