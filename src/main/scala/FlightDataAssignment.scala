/** FlightDataAssignment main program. */
package com.arthuston.flightdata

import org.apache.spark.sql.functions.{col, collect_list, max, month}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.Console.println
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object FlightDataAssignment {

  // column names
  val Month = "Month"
  val NumberFlights = "Number of Flights"
  val PassengerId = "Passenger ID"
  val FirstName = "First Name"
  val LastName = "Last Name"
  val PassengerId1 = "Passenger 1 Id"
  val PassengerId2 = "Passenger 2 Id"
  val NumberFlightsTogether = "Number of Flights Together"
  val LongestRun: String = "Longest Run"
  private val FlightId1: String = "flightId1"
  private val Date1: String = "date1"
  private val FlightId2: String = "flightId2"
  private val Date2: String = "date2"

  def main(args: Array[String]) {
    // input files
    val FlightsCsv = "./input/flightData.csv"
    val PassengersCsv = "./input/passengers.csv"

    // output files
    val TotalNumberOfFlightsEachMonthCsv = "./output/totalNumberOfFlightsEachMonth"
    val NamesOf100MostFrequentFlyersCsv = "./output/namesOfHundredMostFrequentFlyers"
    val GreatestNumberOfCountriesWithoutUKCsv = "./output/greatestNumberOfCountriesWithoutUK"
    val PassengersWithMoreThan3FlightsTogetherCsv = "./output/passengersWithMoreThan3FlightsTogether"

    // start spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // read data
    val flights = new Flights(spark, FlightsCsv)
    val passengers = new Passengers(spark, PassengersCsv)

    // calculations
    showAndSave(TotalNumberOfFlightsEachMonthCsv, totalNumberOfFlightsEachMonth(spark, flights.data()))
    showAndSave(NamesOf100MostFrequentFlyersCsv, namesOf100MostFrequentFlyers(spark, flights.data(), passengers.data()))
    showAndSave(GreatestNumberOfCountriesWithoutUKCsv, greatestNumberOfCountriesWithoutUK(spark, flights.data()))
    showAndSave(PassengersWithMoreThan3FlightsTogetherCsv, passengersWithMoreThan3FlightsTogether(spark, flights.data(), 4))

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
  def namesOf100MostFrequentFlyers(spark: SparkSession, flightsDf: DataFrame, passengersDf: DataFrame, limit: Int = 100): DataFrame = {
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
  def totalNumberOfFlightsEachMonth(spark: SparkSession, flightsDf: DataFrame): DataFrame = {
    flightsDf
      // eliminate duplicate FlightId/Date
      .groupBy(Flights.FlightId, Flights.Date).agg(Map.empty[String, String])
      // convert date to month
      .select(col(Flights.FlightId), month(col(Flights.Date)).alias(Month))
      // sort and group by month
      .sort(col(Month))
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
  def greatestNumberOfCountriesWithoutUK(spark: SparkSession, flightsDf: DataFrame) = {
    val destinationsDf = flightsDf
      .orderBy(Flights.PassengerId, Flights.Date)
      .groupBy(Flights.PassengerId)
      .agg(collect_list(col(Flights.To)).as("destinations"))

    import spark.implicits._
//    import scala.collection.JavaConversions._

    val longestRunDs = destinationsDf.map(row => {
      // get longest run without going to UK
      val passengerId = row.getString(0)
      val destinations = row.getList[String](1).asScala

      var curWithoutUk = 0
      var maxWithoutUk = 0
      for (destination <- destinations) {
        if (destination == "UK") {
          maxWithoutUk = math.max(maxWithoutUk, curWithoutUk)
          curWithoutUk = 0
        } else {
          curWithoutUk += 1
          maxWithoutUk = math.max(maxWithoutUk, curWithoutUk)
        }
      }
      (passengerId, maxWithoutUk)
    })

    longestRunDs.toDF(PassengerId, LongestRun).orderBy(col(LongestRun).desc, col(PassengerId))
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
  def passengersWithMoreThan3FlightsTogether(spark: SparkSession, flightsDf: DataFrame, minFlights: Int = 4) = {
    val passengers1Df = flightsDf
      .withColumnRenamed(Flights.PassengerId, PassengerId1)
      .withColumnRenamed(Flights.FlightId, FlightId1)
      .withColumnRenamed(Flights.Date, Date1)
    val passengers2Df = flightsDf
      .withColumnRenamed(Flights.PassengerId, PassengerId2)
      .withColumnRenamed(Flights.FlightId, FlightId2)
      .withColumnRenamed(Flights.Date, Date2)

    passengers1Df
      .join(
        passengers2Df,
        passengers1Df(FlightId1) === passengers2Df(FlightId2) &&
          passengers1Df(Date1) === passengers2Df(Date2)
      )
      .where(passengers1Df(PassengerId1) < passengers2Df(PassengerId2))
      .groupBy(col(PassengerId1), col(PassengerId2)).count()
      .orderBy(col("count").desc)
      .where(col("count") >= minFlights)
      .withColumnRenamed("count", NumberFlightsTogether)
  }

  private def showAndSave(path: String, df: DataFrame) = {
    println(path)
    df.printSchema()
    df.show(false)
    // coalesce the data so we get a single csv file in a directory
    // coalesce and repartition are expensive
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", value = true).csv(path)
  }
}