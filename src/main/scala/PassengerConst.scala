/**
 * FlightDataAssignment flight data reader.
 * The passengers.csv file has the following columns:
 * Field        Description
 * passengerId  Integer representing the id of a passenger
 * firstName    String representing the first name of a passenger
 * lastName     String representing the last name of a passenger
 */

package com.arthuston.flightdata

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object PassengerConst {
  val FirstName = "firstName"
  val LastName = "lastName"
  val Schema = new StructType(Array(
    StructField(FlightAndPassengerConst.PassengerId, StringType),
    StructField(FirstName, StringType),
    StructField(LastName, StringType),
  ))
  val File = "./input/passengers.csv"
}
