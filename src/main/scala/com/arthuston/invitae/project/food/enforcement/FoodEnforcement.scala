/**
 * Food Enforcement Report main program using API defined at https://open.fda.gov/apis/food/enforcement/
 */
package com.arthuston.invitae.project.food.enforcement

object FoodEnforcement {

  def main(args: Array[String]): Unit = {
    FoodEnforcementAPI.get(limit=FoodEnforcementAPI.MaxLimit)
  }
}
