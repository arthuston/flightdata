/** Quantexa FlightDataAssignment main program. */
package com.arthuston.quantexa.flightdata

import org.apache.spark.sql.functions.{col, collect_list, month}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object FlightDataAssignment {
  // column names
  val Month = "Month"
  val NumberFlights = "Number of Flights"
  val PassengerId = "Passenger ID"
  val FirstName = "First Name"
  val LastName = "Last Name"
  val PassengerId1 = "Passenger 1 Id"
  val PassengerId2 = "Passenger 2 Id"
  val numberFlightsTogether = "Number of Flights Together"

  def main(args: Array[String]) {
    // input files
    val FlightsCsv = "./input/flightData.csv"
    val PassengersCsv = "./input/passengers.csv"

    // output files
    val NumberFightsEachMonthCsv = "./output/numberFlightsEachMonth"
    val NamesOfHundredMostFrequentFlyersCsv = "./output/namesOfHundredMostFrequentFlyers"
    val defCsv = "./output/greatestnumberCountriesWithoutUK.csv"
    val PassengersWithThreeOrMoreFlightsTogetherCsv = "./output/passengersWithThreeOrMoreFlightsTogether"

    // start spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[*]")
      .getOrCreate()

    // read data
    val flights = new Flights(spark, FlightsCsv)
    val passengers = new Passengers(spark, PassengersCsv)

    // calculations
    showAndSave(NumberFightsEachMonthCsv, numberFlightsEachMonth(flights.data()))
    showAndSave(NamesOfHundredMostFrequentFlyersCsv, namesOfMostFrequentFlyers(flights.data(), passengers.data(), 100))
    //    showAndSave(defCsv, greatestNumberCountriesWithoutUK(flights))
    showAndSave(PassengersWithThreeOrMoreFlightsTogetherCsv, passengersWithFlightsTogether(flights.data(), 4))

    spark.stop()
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
  def namesOfMostFrequentFlyers(flightsDf: DataFrame, passengersDf: DataFrame, limit: Int): DataFrame = {
    flightsDf.withColumnRenamed(Flights.PassengerId, PassengerId)
      .join(passengersDf, col(PassengerId) === col(Passengers.PassengerId), "outer")
      .groupBy(PassengerId, Passengers.FirstName, Passengers.LastName)
      .count().sort(col("count").desc)
      .limit(limit)
      .select(
        col(PassengerId),
        col("count").as(NumberFlights),
        col(Passengers.FirstName).as(FirstName),
        col(Passengers.LastName).as(LastName)
      )
  }

  /**
   * Find the total number of flights for each month.
   * The output should be in the following format:
   * Month  Number of Flights
   * 1      123
   * 2      456
   * …      …
   */
  def numberFlightsEachMonth(flightsDf: DataFrame): DataFrame = {
    flightsDf
      // eliminate duplicate flightid/date
      .groupBy(Flights.FlightId, Flights.Date).agg(Map.empty[String, String])
      // convert date to month
      .select(col(Flights.FlightId), month(col(Flights.Date)).alias(Month))
      // sort and group by month
      .sort(Month)
      .groupBy(Month)
      // get number of flights per month
      .count().withColumnRenamed("count", NumberFlights)
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
  def greatestNumberOfCountriesWithoutUK(flights: Flights) = {
    flights.data()
      .orderBy(Flights.PassengerId, Flights.Date)
      .groupBy(Flights.PassengerId, Flights.To)
      .agg(collect_list(col(Flights.To).as("to")))
  }

  /**
   * Find the passengers who have been on more than 3 flights together.
   * Passenger 1 ID   Passenger 2 ID    Number of flights together
   * 56                78                  6
   * 12                34                  8
   * …                 …                   …
   *
   * order the input by 'number of flights flown together in descending order'.
   */
  def passengersWithFlightsTogether(flightsDf: DataFrame, minFlights: Int) = {
    flightsDf
      .withColumnRenamed(Flights.PassengerId, PassengerId1)
      .join(flightsDf.withColumnRenamed(Flights.PassengerId, PassengerId2),
        Seq(Flights.FlightId),
        "inner")
      .where(col(PassengerId1) < col(PassengerId2))
      .groupBy(col(PassengerId1), col(PassengerId2))
      .count()
      .where(col("count") > 3)
      .orderBy(col("count").desc, col(PassengerId1), col(PassengerId2))
      .withColumnRenamed("count", numberFlightsTogether)
  }

  private def showAndSave(path: String, df: DataFrame) = {
    println(path)
    df.show()
    // coalesce the data so we get a single csv file in a directory
    // coalesce and repartition are expensive
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv(path)
  }
}