/**
 * Food Enforcement Report main program using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import org.apache.spark.sql.{Dataset, SparkSession}

object FoodEnforcementReport {

  def main(args: Array[String]): Unit = {

    // constants
    val Top10ClassIIIAlertsByStateAndStatusHeading = "1. Top States with Class III Hazard"
    val Top10ClassIIIAlertsByStateAndStatusFormat = "State %s/Status %s: %d"
    val SectionSeparator = "---"
    val AverageNumberOfReportsPerMonthIn2016Heading = "2. Average reports per month in 2016"
    val AverageNumberOfReportsPerMonthIn2016Format = "%d reports"

    // start spark session to do the data "munging"
    // considered doing different queries here for each of the calculations
    // but decided to use spark to do the data "munging"
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[*]")
      .getOrCreate()

    // read all meta-data and results
    val foodEnforcementData: FoodEnforcementData = FoodEnforcementAPI.get()

    // ignore meta and get the results
    val foodEnforcementResults: Seq[FoodEnforcementResults] = foodEnforcementData.results

    // convert Seq to Spark DataSet
    import spark.implicits._
    val foodEnforcementResultsDs: Dataset[FoodEnforcementResults] = foodEnforcementResults.toDS()

    // calculate and print top10ClassIIIAlertsByStateAndStatus
    println(Top10ClassIIIAlertsByStateAndStatusHeading)
    val top10ClassIIIAlertsByStateAndStatus: Dataset[Top10ClassIIIAlertsByStateAndStatus] =
      FoodEnforcementCalculator.top10ClassIIIAlertsByStateAndStatus(spark, foodEnforcementResultsDs)
    top10ClassIIIAlertsByStateAndStatus.foreach(
      stateStatusCount => {
        println(Top10ClassIIIAlertsByStateAndStatusFormat
          .format(stateStatusCount.state, stateStatusCount.status, stateStatusCount.count)
        )
      }
    )
    println(SectionSeparator)

    // calculate and print averageNumberOfReportsPerMonthIn2016
    println(AverageNumberOfReportsPerMonthIn2016Heading)
    val averageNumberOfReportsPerMonthIn2016: Int =
      FoodEnforcementCalculator.averageNumberOfReportsPerMonthIn2016(spark, foodEnforcementResultsDs)
    println(AverageNumberOfReportsPerMonthIn2016Format.format(averageNumberOfReportsPerMonthIn2016))
    println(SectionSeparator)

    // calculate and print averageNumberOfReportsPerMonthIn2016
    println("3. Top States for 2017")
    val topTenStatesAndTotalReportsIn2017: Dataset[TopTenStatesAndTotalReportsIn2017] =
      FoodEnforcementCalculator.topTenStatesAndTotalReportsIn2017(spark, foodEnforcementResultsDs)
    topTenStatesAndTotalReportsIn2017.foreach(
      stateAndTotalReportsIn2017 => {
        println("%s: %d"
          .format(stateAndTotalReportsIn2017.state, stateAndTotalReportsIn2017.count)
        )
      }
    )
    println(SectionSeparator)

    // done
    spark.stop()
  }
}
