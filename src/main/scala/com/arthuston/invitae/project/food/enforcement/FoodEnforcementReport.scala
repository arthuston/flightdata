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

    // read all meta-data and results
    // considered doing different queries here for each of the calculations
    // but decided to use spark to do the data "munging"
    val foodEnforcementData: FoodEnforcementData = FoodEnforcementAPI.get()

    // ignore meta and get the results
    val foodEnforcementResults: Seq[FoodEnforcementResults] = foodEnforcementData.results

    // start spark session to do the data "munging"
    //
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[*]")
      .getOrCreate()

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

    // done
    spark.stop()
  }
}
