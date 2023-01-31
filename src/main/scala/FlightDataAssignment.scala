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

    // read data
    val flights = readFlights(spark)
    val passengers = readPassengers(spark)

    // calculations
    showAndSave(TotalNumberOfFlightsEachMonthCsv, totalNumberOfFlightsEachMonth(spark, flights))
    showAndSave(NamesOf100MostFrequentFlyersCsv, namesOf100MostFrequentFlyers(spark, flights, passengers))
//    showAndSave(GreatestNumberOfCountriesWithoutUKCsv, greatestNumberOfCountriesWithoutUK(spark, flights)
    showAndSave(PassengersWithMoreThan3FlightsTogetherCsv, passengersWithMoreThan3FlightsTogether(spark, flights, 4))

    spark.stop()
  }

  /**
   * Read flights CSV file
   * @param spark spark session
   * @return Dataset[FlightRaw]
   */
  private def readFlights(spark: SparkSession) = {
    import spark.implicits._
    spark.read
      .option("header", value = true)
      .schema(FlightConst.Schema)
      .csv(FlightConst.File)
      .as[FlightRaw]
  }

  /**
   * Read passengers CSV file
   *
   * @param spark spark session
   * @return Dataset[PassengerRaw]
   */
  private def readPassengers(spark: SparkSession) = {
    import spark.implicits._
    spark.read
      .option("header", value = true)
      .schema(PassengerConst.Schema)
      .csv(PassengerConst.File)
      .as[PassengerRaw]
  }

  /**
   * Find the total number of flights for each month.
   * The output should be in the following format:
   * Month  Number of Flights
   * 1      123
   * 2      456
   * …      …
   *
   * @param spark - spark session*
   * @param flights - flight dataset
   * @return totalNumberOfFlightsEachMonth dataframe
   */
  def totalNumberOfFlightsEachMonth(
    spark: SparkSession,
    flights: Dataset[FlightRaw]
  ): DataFrame = {
    // eliminate duplicate flightID/Date
    val eliminateDupFlights = flights
      .groupBy(FlightConst.FlightId, FlightConst.Date)
      .agg(Map.empty[String, String])

    // convert date to month
    val dateToMonths = eliminateDupFlights
      .select(col(FlightConst.FlightId), month(col(FlightConst.Date)).alias(Month))

    // sort by month ascending
    val sortByMonths = dateToMonths.sort(col(Month))

    // group by month
    val groupByMonths = sortByMonths.groupBy(Month)

    // get number of flights each month
    val numberOfFlightsEachMonths = groupByMonths
      .count().withColumnRenamed("count", NumberFlights)
    numberOfFlightsEachMonths
  }

  /**
   * Find the names of the 100 most frequent flyers.
   * The output should be in the following format:
   * Passenger ID   Number of Flights   First name  Last name
   * 123            100               Firstname   Lastname
   * 456	          75                Firstname   Lastname
   * …              …                 …           …
   *
   * @param spark - spark session
   * @param flights - flight dataset
   * @param passengers - passenger dataset
   * @return namesOf100MostFrequentFlyers dataframe
   */
  def namesOf100MostFrequentFlyers(
    spark: SparkSession,
    flights: Dataset[FlightRaw],
    passengers: Dataset[PassengerRaw],
    limit: Int = 100
  ): DataFrame = {
    // group flights by passengerId and get count
    val passengerFlightCounts = flights
      .groupBy(FlightAndPassengerConst.PassengerId)
      .count()

    // sort by count desc
    val passengerFlightCountSorteds = passengerFlightCounts.sort(col("count").desc)

    // take the first 'limit' counts
    val passengerFlightCountLimits = passengerFlightCountSorteds.limit(limit)

    // join with passengers
    val passengerFlightCountWithNamess = passengerFlightCountLimits.join(passengers, Seq(FlightAndPassengerConst.PassengerId))

    // select output columns
    val passengerFlightCountColss = passengerFlightCountWithNamess.select(
      col(FlightAndPassengerConst.PassengerId).as(PassengerId),
        col("count").as(NumberFlights),
        col(PassengerConst.FirstName).as(FirstName),
        col(PassengerConst.LastName).as(LastName)
      )
    passengerFlightCountColss
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
   *
   * @param spark       - spark session
   * @param flights    - flight dataset
   * @param passengers - passenger dataset
   * @return namesOf100MostFrequentFlyers dataframe
   */
  def greatestNumberOfCountriesWithoutUK(spark: SparkSession, flights: Dataset[FlightRaw]) = {
    val destinationss = flights
      .orderBy(FlightAndPassengerConst.PassengerId, FlightConst.Date)
      .groupBy(FlightAndPassengerConst.PassengerId)
      .agg(collect_list(col(FlightConst.To)).as("destinations"))

    import spark.implicits._

    val longestRuns = destinationss.map(row => {
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

    longestRuns.toDF(PassengerId, LongestRun).orderBy(col(LongestRun).desc, col(PassengerId))
  }

  /**
   * Find the passengers who have been on more than 3 flights together.
   * Passenger 1 ID   Passenger 2 ID    Number of flights together
   * 56                78                  6
   * 12                34                  8
   * …                 …                   …
   *
   * order the input by 'number of flights flown together in descending order'.
   *
   * @param spark spark session
   * @param flights flight dataset
   * @param minFlights minimum number of flights together
   */
  def passengersWithMoreThan3FlightsTogether(
    spark: SparkSession,
    flights: Dataset[FlightRaw],
    minFlights: Int = 4
  ) = {
    val passengers1s = flights
      .withColumnRenamed(FlightAndPassengerConst.PassengerId, PassengerId1)
      .withColumnRenamed(FlightConst.FlightId, FlightId1)
      .withColumnRenamed(FlightConst.Date, Date1)
    val passengers2s = flights
      .withColumnRenamed(FlightAndPassengerConst.PassengerId, PassengerId2)
      .withColumnRenamed(FlightConst.FlightId, FlightId2)
      .withColumnRenamed(FlightConst.Date, Date2)

    passengers1s
      .join(
        passengers2s,
        passengers1s(FlightId1) === passengers2s(FlightId2) &&
          passengers1s(Date1) === passengers2s(Date2)
      )
      .where(passengers1s(PassengerId1) < passengers2s(PassengerId2))
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