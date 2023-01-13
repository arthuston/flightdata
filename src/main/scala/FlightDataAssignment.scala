/** Quantexa FlightDataAssignment main program. */
package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.functions.{col, month}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object FlightDataAssignment {
  // column names
  val Month = "Month"
  val NumberOfFlights = "Number of Flights"
  val PassengerId = "Passenger ID"
  val FirstName = "First Name"
  val LastName = "Last Name"
  val PassengerId1 = "Passenger 1 Id"
  val PassengerId2 = "Passenger 2 Id"
  val NumberOfFlightsTogether = "Number of Flights Together"

  def main(args: Array[String]) {
    // input files
    val FlightsCsv = "./input/flightData.csv"
    val PassengersCsv = "./input/passengers.csv"

    // output files
    val TotalNumberOfFlightsEachMonthCsv = "./output/totalNumberOfFlightsEachMonth.csv"
    val NamesOfHundredMostFrequentFlyersCsv = "./output/namesOfTheHundredMostFrequentFlyers.csv"
    val GreatestNumberOfCountriesWithoutUKCsv = "./output/greatestNumberOfCountriesWithoutUK.csv"
    val PassengersWithThreeOrMoreFlightsTogetherCsv = "./output/passengersWithThreeOrMoreFlightsTogether.csv"

    // start spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[*]")
      .getOrCreate()

    // read data
    val flights = new FlightData(spark, FlightsCsv)
    flights.data().show()
    val passengers = new Passsengers(spark, PassengersCsv)
    passengers.data().show()

    // calculations
//    showAndSave(TotalNumberOfFlightsEachMonthCsv, totalNumberOfFlightsEachMonth(flights))
//    showAndSave(NamesOfHundredMostFrequentFlyersCsv, namesOfTheHundredMustFrequentFlyers(flights, passengers))
//    showAndSave(GreatestNumberOfCountriesWithoutUKCsv, greatestNumberOfCountriesWithoutUK(FlightData))
    showAndSave(PassengersWithThreeOrMoreFlightsTogetherCsv, passengersWithThreeOrMoreFlightsTogether(flights))

    spark.stop()
  }

  /**
   * Find the total number of flights for each month.
   * The output should be in the following format:
   * Month  Number of Flights
   * 1      123
   * 2      456
   * …      …
   */
  private def totalNumberOfFlightsEachMonth(flights: FlightData): Dataset[Row] = {
    val flightsDf = flights.data()
    flightsDf.select(
      month(col(flights.Date)).alias(Month),
      col(flights.FlightId)
    ).distinct().sort(Month)
      .groupBy(Month).count()
      .withColumnRenamed("count", NumberOfFlights)
  }

  /**
   * Find the names of the 100 most frequent flyers.
   * The output should be in the following format:
   * Passenger ID   Number of Flights   First name  Last name
   * 123            100               Firstname   Lastname
   * 456	          75                Firstname   Lastname
   * …              …                 …           …
   *
   * @param flights
   * @param passengers
   * @return
   */
  private def namesOfTheHundredMustFrequentFlyers(flights: FlightData, passengers: Passsengers): DataFrame = {
    val flightDf = flights.data()
    val passengersDf = passengers.data()
    flightDf.withColumnRenamed(flights.PassengerId, PassengerId)
      .join(passengersDf, col(PassengerId) === col(passengers.PassengerId), "outer")
      .groupBy(PassengerId, passengers.FirstName, passengers.LastName)
      .count().sort(col("count").desc)
      .limit(100)
      .select(
        col(PassengerId),
        col("count").as(NumberOfFlights),
        col(passengers.FirstName).as(FirstName),
        col(passengers.LastName).as(LastName)
      )
  }

  /**
   * Find the greatest of countries a passenger has been in without being in the UK.
   * For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK,
   * the correct answer would be 3 countries.
   * The output should be in the following format:
   * Passenger ID   Longest Run
   * 45              4
   * 23              6
   * …               …
   *
   * order the input by 'longest run in descending order'
   */
  private def greatestNumberOfCountriesWithoutUK(flights: FlightData) = {
    flights.data()
  }

  /**
   * Find the passengers who have been on more than 3 flights together.
   * The output should be in the following format:
   * Passenger 1 ID   Passenger 2 ID    Number of flights together
   * 56                78                  6
   * 12                34                  8
   * …                 …                   …
   *
   * order the input by 'number of flights flown together in descending order'.
   */
  private def passengersWithThreeOrMoreFlightsTogether(flights: FlightData) = {
    val flightsDf = flights.data()
    flightsDf
      .withColumnRenamed(flights.PassengerId, PassengerId1)
      .join(flightsDf.withColumnRenamed(flights.PassengerId, PassengerId2),
        Seq(flights.FlightId),
        "inner")
      .where(col(PassengerId1) < col(PassengerId2))
      .groupBy(col(PassengerId1), col(PassengerId2))
      .count()
      .where(col("count") > 3)
      .withColumnRenamed("count", NumberOfFlightsTogether)
      .orderBy(col(NumberOfFlightsTogether).desc, col(PassengerId1), col(PassengerId2))
  }

  private def showAndSave(path: String, df: DataFrame) = {
    println(path)
    df.show()
    // coalesce the data so we get a single csv file in a directory
    // coalesce and repartition are expensive
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv(path)
  }
}