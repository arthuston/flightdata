/**
 * Food Enforcement API data using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

import play.api.libs.json.{JsPath, Reads}
import play.api.libs.functional.syntax._ // Combinator syntax

// case classes generated using https://api.fda.gov/food/enforcement.json?limit=1000 and https://transform.tools/json-to-scala-case-class
case class FoodEnforcementMeta(
                                disclaimer: String,
                                terms: String,
                                license: String,
                                last_updated: String,
                                results: FoodEnforcementMetaResults
                              )

case class FoodEnforcementMetaResults(
                                       skip: Int,
                                       limit: Int,
                                       total: Int
                                     )

// Omitting the fields that are not needed in the coding test.
// I'm using play-json for unmarshalling from json to case classes which is limited to 22 fields
case class FoodEnforcementResults(
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
                                   // openfda: Any,
                                   recalling_firm: String,
                                   recall_number: String,
                                   initial_firm_notification: String,
                                   product_type: String,
                                   event_id: String //,
//                                   more_code_info: Option[String],
//                                   recall_initiation_date: String,
//                                   postal_code: String,
//                                   voluntary_mandated: String,
//                                   status: String,
//                                   termination_date: Option[String]
                                 )

case class FoodEnforcementData(
                                meta: FoodEnforcementMeta,
                                results: Seq[FoodEnforcementResults]
                              )


object FoodEnforcementDataImplicits {
  implicit val foodEnforcementMetaResultsReads: Reads[FoodEnforcementMetaResults] = (
    (JsPath \ "skip").read[Int] and
      (JsPath \ "limit").read[Int] and
      (JsPath \ "total").read[Int]
    )(FoodEnforcementMetaResults.apply _)
  implicit val foodEnforcementMetaReads: Reads[FoodEnforcementMeta] = (
    (JsPath \ "disclaimer").read[String] and
      (JsPath \ "terms").read[String] and
      (JsPath \ "license").read[String] and
      (JsPath \ "last_updated").read[String] and
      (JsPath \ "results").read[FoodEnforcementMetaResults]
    )(FoodEnforcementMeta.apply _)
  implicit val FoodEnforcementResultsReads: Reads[FoodEnforcementResults] = (
    (JsPath \ "country").read[String] and
      (JsPath \ "city").read[String] and
      (JsPath \ "address_1").read[String] and
      (JsPath \ "reason_for_recall").read[String] and
      (JsPath \ "address_2").read[String] and
      (JsPath \ "product_quantity").read[String] and
      (JsPath \ "code_info").read[String] and
      (JsPath \ "center_classification_date").read[String] and
      (JsPath \ "distribution_pattern").read[String] and
      (JsPath \ "state").read[String] and
      (JsPath \ "product_description").read[String] and
      (JsPath \ "report_date").read[String] and
      (JsPath \ "classification").read[String] and
      (JsPath \ "recalling_firm").read[String] and
      (JsPath \ "recall_number").read[String] and
      (JsPath \ "initial_firm_notification").read[String] and
      (JsPath \ "product_type").read[String] and
      (JsPath \ "event_id").read[String]
    )(FoodEnforcementResults.apply _)
  //  implicit val FoodEnforcementResultsSeqReads: Reads[Seq[FoodEnforcementResults]] = Reads.seq(FoodEnforcementResultsReads)
  implicit val foodEnforcementDataReads: Reads[FoodEnforcementData] = (
    (JsPath \ "meta").read[FoodEnforcementMeta] and
      (JsPath \ "meta").read[Seq[FoodEnforcementResults]]
    )(FoodEnforcementData.apply _)

}
