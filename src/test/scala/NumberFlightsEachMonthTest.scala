/** Quantexa FlightDataAssignment main program test. */
package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

import java.sql.Date
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
class NumberFlightsEachMonthTest extends FunSuite {
  // Setup
  val ExpectedSchema = StructType(
    Array(
      StructField(FlightDataAssignment.Month, IntegerType),
      StructField(FlightDataAssignment.NumberFlights, IntegerType)
    )
  )

  val spark = SparkSession
    .builder()
    .appName("Spark SQL data sources example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test(testName = "testNumberFlightsEachMonth") {
    val input = spark.createDataFrame(Seq(
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
    ), Flights.Schema)

    val expected = spark.createDataFrame(
      Seq(
        Row(1, 1),
        Row(2, 2),
        Row(3, 3),
        Row(4, 4)
      ),
      ExpectedSchema
    )

    assertResult(expected) {
      FlightDataAssignment.numberFlightsEachMonth(input)
    }
  }
}


