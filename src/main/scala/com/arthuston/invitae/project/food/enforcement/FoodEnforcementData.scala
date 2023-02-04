/**
 * Food Enforcement API data using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import akka.NotUsed
import spray.json.DefaultJsonProtocol

final case class FoodEnforcementMetaResults(
                                       skip: Int,
                                       limit: Int,
                                       total: Int
                                     )

final case class FoodEnforcementMeta(
                                disclaimer: String,
                                terms: String,
                                license: String,
                                last_updated: String,
                                results: FoodEnforcementMetaResults
                              )

final case class FoodEnforcementResults(
                                   country: String,
                                   city: String,
                                   address_1: String,
                                   reason_for_recall: String,
                                   address_2: String,
                                   product_quantity: String,
                                   code_info: String,
                                   center_classification_date: String,
                                   distribution_pattern: String,
                                   state: String,
                                   product_description: String,
                                   report_date: String,
                                   classification: String,
                                   openfda: Any,
                                   recalling_firm: String,
                                   recall_number: String,
                                   initial_firm_notification: String,
                                   product_type: String,
                                   event_id: String,
                                   more_code_info: String,
                                   recall_initiation_date: String,
                                   postal_code: String,
                                   voluntary_mandated: String,
                                   status: String
                                 )

final case class FoodEnforcementData(
                                meta: FoodEnforcementMeta,
                                results: Seq[FoodEnforcementResults]
                              )

object FoodEnforcementDataJsonProtocol
  extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
    with spray.json.DefaultJsonProtocol {

  implicit val foodEnforcementMetaResults = jsonFormat2(FoodEnforcementMetaResults.apply)
}


