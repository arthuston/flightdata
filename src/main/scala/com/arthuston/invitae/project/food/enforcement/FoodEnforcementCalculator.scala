/**
 * Food Enforcement API calculator using spark for data-munging.
 */

package com.arthuston.invitae.project.food.enforcement

import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.col

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
   * @param foodEnforcementResultsDs spark FoodEnforcementResults spark DataSet
   * @return result spark DataFrame
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

    // limit 10
    val limit10: Dataset[Top10ClassIIIAlertsByStateAndStatus] = orderByCountDescending.limit(10)
    limit10
  }
}
