/** Quantexa FlightDataAssignment main program test. */
package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.sql.Date
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

import java.sql.Date
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._

class FlightDataAssignmentTest extends FunSuite {
  // Setup
  val spark = SparkSession
    .builder()
    .appName("Spark SQL data sources example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test(testName = "testNumberFlightsEachMonth") {
    println("testNumberFlightsEachMonth")

    val inputFlights = spark.createDataFrame(
      Seq(
        // two passengers same flight and day
        Row("pass1", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass2", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        // same passengers same flight different days
        Row("pass3", "flight2", "from", "to", Date.valueOf("2017-02-01")),
        Row("pass3", "flight2", "from", "to", Date.valueOf("2017-02-02")),
        // different passengers same flight different days
        Row("pass4", "flight3", "from", "to", Date.valueOf("2017-03-01")),
        Row("pass5", "flight3", "from", "to", Date.valueOf("2017-03-02")),
        Row("pass6", "flight3", "from", "to", Date.valueOf("2017-03-03")),
        // same passengers different flight same days
        Row("pass7", "flight4", "from", "to", Date.valueOf("2017-04-01")),
        Row("pass7", "flight5", "from", "to", Date.valueOf("2017-04-02")),
        Row("pass7", "flight6", "from", "to", Date.valueOf("2017-04-03")),
        Row("pass7", "flight7", "from", "to", Date.valueOf("2017-04-04"))
      ),
      Flights.Schema)

    val ExpectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.Month, IntegerType, true),
        StructField(FlightDataAssignment.NumberFlights, LongType, false)
      )
    )

    val expected = spark.createDataFrame(
      Seq(
        Row(1, 1L),
        Row(2, 2L),
        Row(3, 3L),
        Row(4, 4L)
      ),
      ExpectedSchema
    )

    val actual = FlightDataAssignment.numberFlightsEachMonth(inputFlights)

    expected.show()
    actual.show()

    // TODO: Expected [Month: int, Number of Flights: bigint], but got [Month: int, Number of Flights: bigint]
    //    assertResult(expected) {
    //      actual
    //    }
  }

  test(testName = "testNamesOfMostFrequentFlyers") {
    println("testNamesOfMostFrequentFlyers")

    val inputFlights = spark.createDataFrame(
      Seq(
        // passenger 1 (1 flight: 1 flightId))
        Row("pass1", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        // passenger 2 (2 flights: 1 flightId, twice on same day)
        Row("pass2", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass2", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        // passenger 3 (3 flights: 1 flightId, 3 different days)
        Row("pass3", "flight2", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass3", "flight2", "from", "to", Date.valueOf("2017-01-02")),
        Row("pass3", "flight2", "from", "to", Date.valueOf("2017-01-03")),
        // passenger 4 (4 flights: 4 flightId, same day)
        Row("pass4", "flight3", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass4", "flight4", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass4", "flight5", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass4", "flight6", "from", "to", Date.valueOf("2017-01-01")),

      ),
      Flights.Schema
    )
    val inputPassengers = spark.createDataFrame(
      Seq(
        // passenger 1
        Row("pass1", "firstname1", "lastname1"),
        Row("pass2", "firstname2", "lastname2"),
        Row("pass3", "firstname3", "lastname3"),
        Row("pass4", "firstname4", "lastname4"),
      ),
      Passengers.Schema)

    val ExpectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.PassengerId, StringType),
        StructField(FlightDataAssignment.NumberFlights, LongType),
        StructField(FlightDataAssignment.FirstName, StringType),
        StructField(FlightDataAssignment.LastName, StringType)
      )
    )

    val expected1 = spark.createDataFrame(
      Seq(
        Row("pass4", 4L, "firstname4", "lastname4"),
        Row("pass3", 3L, "firstname3", "lastname3"),
        Row("pass2", 2L, "firstname2", "lastname2"),
        Row("pass2", 1L, "firstname1", "lastname1"),
      ),
      ExpectedSchema
    )

    val actual1 = FlightDataAssignment.namesOf100MostFrequentFlyers(inputFlights, inputPassengers, 4)

    expected1.show()
    actual1.show()

    // TODO: Fix Expected [Passenger ID: string, Number of Flights: bigint ... 2 more fields], but got [Passenger ID: string, Number of Flights: bigint ... 2 more fields]
    //    assertResult(expected1) {
    //      actual1
    //    }

    val expected2 = spark.createDataFrame(
      Seq(
        Row("pass4", 4L, "firstname4", "lastname4"),
        Row("pass3", 3L, "firstname3", "lastname3"),
        Row("pass2", 2L, "firstname2", "lastname2"),
      ),
      ExpectedSchema
    )

    val actual2 = FlightDataAssignment.namesOf100MostFrequentFlyers(inputFlights, inputPassengers, 3)

    expected2.show()
    actual2.show()

    // TODO: Fix Expected [Passenger ID: string, Number of Flights: bigint ... 2 more fields], but got [Passenger ID: string, Number of Flights: bigint ... 2 more fields]
    //    assertResult(expected2) {
    //      actual2
    //    }
  }
}


