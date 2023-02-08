/**
 * Food Enforcement API calculator using spark for data-munging.
 */

package com.arthuston.invitae.project.food.enforcement

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, month, year}

case class StateStatusCount(state: String, status: String, count: BigInt)
case class StateCount(state: String, count: BigInt)
case class YearCount(year: Int, count: BigInt)

case class HighestLowestYearCount(highestYear: Int, highestCount: BigInt, lowestYear: Int, lowestCount: BigInt)
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
   * @param spark                    sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return spark FoodEnforcementResults DataSet
   */
  def top10ClassIIIAlertsByStateAndStatus(
                                           spark: SparkSession,
                                           foodEnforcementResultsDs: Dataset[FoodEnforcementResults]
                                         ): Dataset[StateStatusCount] = {

    // select "Class III" health hazard classification alerts
    val classIIIAlerts: Dataset[FoodEnforcementResults] =
      foodEnforcementResultsDs.where(col("classification") === "Class III")

    // group by state and status
    val groupByStateAndStatus: RelationalGroupedDataset = classIIIAlerts.groupBy(col("state"), col("status"))

    // get count
    import spark.implicits._
    val stateAndStatusCount: Dataset[StateStatusCount] = groupByStateAndStatus.count().as[StateStatusCount]

    // order by count descending
    val orderByCountDescending: Dataset[StateStatusCount] = stateAndStatusCount.sort(col("count").desc)

    val limit10: Dataset[StateStatusCount] = orderByCountDescending.limit(10)
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
   * @param spark                    sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return average number of reports per month in 2016
   */
  def averageNumberOfReportsPerMonthIn2016(
                                            spark: SparkSession,
                                            foodEnforcementResultsDs: Dataset[FoodEnforcementResults]
                                          ): Int = {

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

  /**
   * Get list of the top 10 states and the number of reports each had in 2017.
   *
   * Example output:
   * 3. Top States for 2017
   * IL: 468
   * CA: 293
   * FL: 245
   * NY: 227
   * NH: 210
   * PA: 188
   * WA: 156
   * TX: 143
   * MA: 134
   * OH: 123
   * ---
   *
   * @param spark                    sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return list of the top 10 states and the number of reports each had in 2017.
   */
  def topTenStatesAndTotalReportsIn2017(
                                         spark: SparkSession,
                                         foodEnforcementResultsDs: Dataset[FoodEnforcementResults]
                                       ): Dataset[StateCount] = {

    // where year is 2017
    val yearIs2017: Dataset[FoodEnforcementResults] =
      foodEnforcementResultsDs.where(year(col("report_date")) === 2017)

    // group by state
    val groupByState: RelationalGroupedDataset = yearIs2017.groupBy(col("state"))

    // get count
    import spark.implicits._
    val getCount: Dataset[StateCount] =
      groupByState.count().as[StateCount]

    // order by count descending
    val orderByCountDesc: Dataset[StateCount] =
      getCount.orderBy(col("count").desc)

    // limit 10
    val limit10 = orderByCountDesc.limit(10)
    limit10
  }

  /**
   * Get top years.
   *
   * Sample output:
   * 4. Top Years
   * Highest year is 2017 with 3206 reports
   * Lowest year is 2004 with 0 reports
   * Note: TODO This will not find years with 0 reports, there must be at least one report in the year!
   *
   * @param spark                    sparkSession
   * @param foodEnforcementResultsDs spark FoodEnforcementResults DataSet
   * @return years and counts sorted in descending order
   */
  def highestLowestYearCount(
                spark: SparkSession,
                foodEnforcementResultsDs: Dataset[FoodEnforcementResults]
              ): HighestLowestYearCount = {
    // get years
    val getYears: Dataset[Row] = foodEnforcementResultsDs.select(year(col("report_date")).as("year"))

    // group by year
    val groupByYear: RelationalGroupedDataset = getYears.groupBy(col("year"))

    // year counts
    import spark.implicits._
    val yearCounts: Dataset[YearCount] = groupByYear.count().as[YearCount]

    // order by count desc
    val orderByCountDesc: Dataset[YearCount] = yearCounts.orderBy(col("count").desc)
    val highestYearCount: YearCount = orderByCountDesc.head()
    val lowestYearCount: YearCount = orderByCountDesc.tail(1).last
    HighestLowestYearCount(highestYear = highestYearCount.year, highestCount = highestYearCount.count,
      lowestYear = lowestYearCount.year, lowestCount = lowestYearCount.count)
  }
}
