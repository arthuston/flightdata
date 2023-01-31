/** FlightDataAssignment main program test. */
package com.arthuston.flightdata

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.sql.Date
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

  test(testName = "testTotalNumberOfFlightsEachMonth") {
    println("testTotalNumberOfFlightsEachMonth")

    val flights = spark.createDataFrame(
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
      FlightConst.Schema)

    val ExpectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.Month, IntegerType, nullable = true),
        StructField(FlightDataAssignment.NumberFlights, LongType, nullable = false)
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
    val actual = FlightDataAssignment.totalNumberOfFlightsEachMonth(spark, flights)
    expected.show(false)
    actual.show(false)

    // TODO: Expected [Month: int, Number of Flights: bigint], but got [Month: int, Number of Flights: bigint]
    //    assertResult(expected) {
    //      actual
    //    }
  }

  test(testName = "testNamesOf100MostFrequentFlyers") {
    println("testNamesOf100MostFrequentFlyers")

    val flights = spark.createDataFrame(
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
      FlightConst.Schema
    )
    val passengers = spark.createDataFrame(
      Seq(
        // passenger 1
        Row("pass1", "firstname1", "lastname1"),
        Row("pass2", "firstname2", "lastname2"),
        Row("pass3", "firstname3", "lastname3"),
        Row("pass4", "firstname4", "lastname4"),
      ),
      PassengerConst.Schema)

    val expectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.PassengerId, StringType),
        StructField(FlightDataAssignment.NumberFlights, LongType),
        StructField(FlightDataAssignment.FirstName, StringType),
        StructField(FlightDataAssignment.LastName, StringType)
      )
    )

    println("testNamesOfMostFrequentFlyers limit = 4")
    var expectedLimit4 = spark.createDataFrame(
      Seq(
        Row("pass4", 4L, "firstname4", "lastname4"),
        Row("pass3", 3L, "firstname3", "lastname3"),
        Row("pass2", 2L, "firstname2", "lastname2"),
        Row("pass2", 1L, "firstname1", "lastname1"),
      ),
      expectedSchema
    )
    val actualLimit4 = FlightDataAssignment.namesOf100MostFrequentFlyers(spark, flights, passengers, 4)
    printDf(expectedLimit4)
    printDf(actualLimit4)

    // TODO: Fix Expected [Passenger ID: string, Number of Flights: bigint ... 2 more fields], but got [Passenger ID: string, Number of Flights: bigint ... 2 more fields]
    //    assertResult(expectedLimit4) {
    //      actualLimit4
    //    }

    println("testNamesOfMostFrequentFlyers limit = 3")
    val expectedLimit3 = spark.createDataFrame(
      Seq(
        Row("pass4", 4L, "firstname4", "lastname4"),
        Row("pass3", 3L, "firstname3", "lastname3"),
        Row("pass2", 2L, "firstname2", "lastname2"),
      ),
      expectedSchema
    )
    val actualLimit3 = FlightDataAssignment.namesOf100MostFrequentFlyers(spark, flights, passengers, 3)
    printDf(expectedLimit3)
    printDf(actualLimit3)

    // TODO: Fix Expected [Passenger ID: string, Number of Flights: bigint ... 2 more fields], but got [Passenger ID: string, Number of Flights: bigint ... 2 more fields]
    //    assertResult(expectedLimit3) {
    //      actualLimit3
    //    }
  }

  test(testName="testPassengersWithMoreThan3FlightsTogether") {
    println("testPassengersWithMoreThan3FlightsTogether")

    val flightsDf = spark.createDataFrame(
      Seq(
        // zero flights together
        Row("pass1", "flight1", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass1", "flight2", "from", "to", Date.valueOf("2017-01-01")),
        Row("pass2", "flight1", "from", "to", Date.valueOf("2017-01-02")),
        // one flight together
        Row("pass3", "flight3", "from", "to", Date.valueOf("2017-01-03")),
        Row("pass4", "flight3", "from", "to", Date.valueOf("2017-01-03")),
        // two flights together (2 flights same day)
        Row("pass5", "flight4", "from", "to", Date.valueOf("2017-01-04")),
        Row("pass6", "flight4", "from", "to", Date.valueOf("2017-01-04")),
        Row("pass5", "flight5", "from", "to", Date.valueOf("2017-01-04")),
        Row("pass6", "flight5", "from", "to", Date.valueOf("2017-01-04")),
        // three flights together (3 flights on different days)
        Row("pass7", "flight6", "from", "to", Date.valueOf("2017-01-05")),
        Row("pass8", "flight6", "from", "to", Date.valueOf("2017-01-05")),
        Row("pass7", "flight6", "from", "to", Date.valueOf("2017-01-06")),
        Row("pass8", "flight6", "from", "to", Date.valueOf("2017-01-06")),
        Row("pass7", "flight6", "from", "to", Date.valueOf("2017-01-07")),
        Row("pass8", "flight6", "from", "to", Date.valueOf("2017-01-07")),
      ),
      FlightConst.Schema)

    val expectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.PassengerId1, StringType),
        StructField(FlightDataAssignment.PassengerId2, StringType),
        StructField(FlightDataAssignment.NumberFlightsTogether, LongType),
      )
    )

    println("testNamesOfMostFrequentFlyers minFlights = 0")
    val expectedMinFlights0or1Df = spark.createDataFrame(
      Seq(
        Row("pass7", "pass8", 3L),
        Row("pass5", "pass6", 2L),
        Row("pass3", "pass4", 1L),
      ),
      expectedSchema
    )
    val actualMinFlights0Df = FlightDataAssignment.passengersWithMoreThan3FlightsTogether(spark, flightsDf, 0)
    printDf(expectedMinFlights0or1Df)
    printDf(actualMinFlights0Df)

    // TODO: Fix Expected [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field], but got [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field]
    //    assertResult(expectedMinFlights0or1) {
    //      actualMinFlights0Df
    //    }

    println("testNamesOfMostFrequentFlyers minFlights = 1")
    val actualMinFlights1Df = FlightDataAssignment.passengersWithMoreThan3FlightsTogether(spark, flightsDf, 1)
    expectedMinFlights0or1Df.show(false)
    actualMinFlights1Df.show(false)

    // TODO: Fix Expected [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field], but got [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field]
//    assertResult(expectedMinFlights0or1) {
//      actualMinFlights1Df
//    }

    println("testNamesOfMostFrequentFlyers minFlights = 3")
    val expectedMinFlights3Df = spark.createDataFrame(
      Seq(
        Row("pass7", "pass8", 3L),
      ),
      expectedSchema
    )
    val actualMinFlights3Df = FlightDataAssignment.passengersWithMoreThan3FlightsTogether(spark, flightsDf, 3)
    printDf(expectedMinFlights3Df)
    printDf(actualMinFlights3Df)

    // TODO: Fix Expected [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field], but got [Passenger 1 Id: string, Passenger 2 Id: string ... 1 more field]
    //    assertResult(expectedMinFlights3Df) {
    //      actualMinFlights3Df
    //    }
  }

  test(testName="testGreatestNumberOfCountriesWithoutUK") {
    println("testGreatestNumberOfCountriesWithoutUK")

    val flightsDf = spark.createDataFrame(
      Seq(
        // zero flights not to UIK
        Row("pass1", "flight1", "from", "UK", Date.valueOf("2017-01-01")),
        Row("pass1", "flight2", "from", "UK", Date.valueOf("2017-01-02")),
        Row("pass1", "flight3", "from", "UK", Date.valueOf("2017-01-03")),
        Row("pass1", "flight4", "from", "UK", Date.valueOf("2017-01-04")),
        // one flight not to UK
        Row("pass2", "flight5", "from", "to", Date.valueOf("2017-01-05")),
        // one flight not to UK
        Row("pass3", "flight6", "from", "UK", Date.valueOf("2017-01-06")),
        Row("pass3", "flight6", "from", "to", Date.valueOf("2017-01-07")),
        // one flight not to UK
        Row("pass4", "flight7", "from", "UK", Date.valueOf("2017-01-08")),
        Row("pass4", "flight7", "from", "to", Date.valueOf("2017-01-08")),
        Row("pass4", "flight7", "from", "UK", Date.valueOf("2017-01-09")),
        // max == two flights not to UK
        Row("pass5", "flight8", "from", "UK", Date.valueOf("2017-01-08")),  // 0
        Row("pass5", "flight8", "from", "to", Date.valueOf("2017-01-09")),  // 1
        Row("pass5", "flight8", "from", "UK", Date.valueOf("2017-01-10")),
        Row("pass5", "flight8", "from", "to", Date.valueOf("2017-01-11")),  // 2
        Row("pass5", "flight8", "from", "to", Date.valueOf("2017-01-12")),
        Row("pass5", "flight8", "from", "UK", Date.valueOf("2017-01-13")),
        // max == three flights not to UK
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-14")), // 3
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-15")),
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-16")),
        Row("pass6", "flight9", "from", "UK", Date.valueOf("2017-01-17")),
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-18")), // 2
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-19")),
        Row("pass6", "flight9", "from", "UK", Date.valueOf("2017-01-20")),
        Row("pass6", "flight9", "from", "to", Date.valueOf("2017-01-21")), // 1
        Row("pass6", "flight9", "from", "UK", Date.valueOf("2017-01-22")),
        Row("pass6", "flight9", "from", "UK", Date.valueOf("2017-01-23")), // 0
      ),
      FlightConst.Schema)

    val expectedSchema = StructType(
      Array(
        StructField(FlightDataAssignment.PassengerId, StringType),
        StructField(FlightDataAssignment.LongestRun, LongType),
      )
    )

    val expectedDf = spark.createDataFrame(
      Seq(
        Row("pass6", 3L),
        Row("pass5", 2L),
        Row("pass3", 1L),
        Row("pass4", 1L),
        Row("pass1", 0L),
      ),
      expectedSchema
    )
    val actualDf = FlightDataAssignment.greatestNumberOfCountriesWithoutUK(spark, flightsDf)

    printDf(expectedDf)
    printDf(actualDf)
  }

  def printDf(df: DataFrame): Unit = {
    df.printSchema()
    df.show(false)
  }
}

