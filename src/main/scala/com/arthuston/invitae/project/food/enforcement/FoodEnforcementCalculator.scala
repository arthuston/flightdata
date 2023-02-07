/**
 * Food Enforcement API calculator using spark for data-munging.
 */

package com.arthuston.invitae.project.food.enforcement

import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, month, year}

// list of top 10 "Class III" health hazard classification alerts, grouped by state and
// further by status.
case class Top10ClassIIIAlertsByStateAndStatus(state: String, status: String, count: BigInt)

// food enforcement calculator
object FoodEnforcementCalculator {


  /**
   * Get a list of top 10 "Class III" health hazard classification alerts, grouped by state and
   * further by status.
   *
   * Example output:
   * 1. Top States with Class III Hazard
   * State CA/Status Terminated: 164
   * State TX/Status Terminated: 130
   * State IA/Status Terminated: 115
   * State WA/Status Terminated: 100
   * State MA/Status Terminated: 86
   * State NY/Status Terminated: 71
   * State NJ/Status Terminated: 50
   * State OR/Status Terminated: 32
   * State UT/Status Terminated: 28
   * State FL/Status Terminated: 27
   * ---
   *
   * @param spark sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return spark FoodEnforcementResults DataSet
   */
  def top10ClassIIIAlertsByStateAndStatus(spark: SparkSession, foodEnforcementResultsDs: Dataset[FoodEnforcementResults]): Dataset[Top10ClassIIIAlertsByStateAndStatus] = {

    // select "Class III" health hazard classification alerts
    val classIIIAlerts: Dataset[FoodEnforcementResults] =
      foodEnforcementResultsDs.where(col("classification") === "Class III")

    // group by state and status
    val groupByStateAndStatus: RelationalGroupedDataset = classIIIAlerts.groupBy(col("state"), col("status"))

    // get count
    import spark.implicits._
    val stateAndStatusCount: Dataset[Top10ClassIIIAlertsByStateAndStatus] = groupByStateAndStatus.count().as[Top10ClassIIIAlertsByStateAndStatus]

    // order by count descending
    val orderByCountDescending: Dataset[Top10ClassIIIAlertsByStateAndStatus] = stateAndStatusCount.sort(col("count").desc)

    val limit10: Dataset[Top10ClassIIIAlertsByStateAndStatus] = orderByCountDescending.limit(10)
    limit10
  }

  /**
   * Calculate the average number of reports per month in 2016
   * case class AverageNumberOfReportsPerMonthIn2016(count: BigInt)
   *
   * Example output:
   * 2. Average reports per month in 2016
   * 251 reports
   * ---
   *
   * @param spark sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return average number of reports per month in 2016
   */
  def averageNumberOfReportsPerMonthIn2016(spark: SparkSession, foodEnforcementResultsDs: Dataset[FoodEnforcementResults]): Int = {

    // where year is 2016
    val yearIs2016: Dataset[FoodEnforcementResults] = foodEnforcementResultsDs.where(year(col("report_date")) === 2016)

    // get count
    val getCount: Long = yearIs2016.count()

    // get Average Per Month
    val averagePerMonthFloat = getCount / 12.0

    // round to nearest Int
    val averagePerMonthInt = (averagePerMonthFloat + 0.5).toInt
    averagePerMonthInt
  }
}

