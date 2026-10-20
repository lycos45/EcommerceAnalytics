package com.ecommerce.analytics

import org.scalatest.funsuite.AnyFunSuite

/** Tests de la fonction pure TimeFeatures.extract (Membre B) : pas besoin de Spark. */
class TimeFeaturesSpec extends AnyFunSuite {

  test("une date valide donne les bonnes caractéristiques (lundi 1er juillet 2024, 02:18)") {
    val f = TimeFeatures.extract("20240701021822").get
    assert(f.hour == 2)
    assert(f.day_of_week == "Monday")
    assert(f.month == "July")
    assert(f.is_weekend == 0)
    assert(f.day_period == "Night")
    assert(f.is_working_hours == 0)
  }

  test("samedi après-midi : week-end, Afternoon, heures de bureau") {
    val f = TimeFeatures.extract("20240706140000").get   // samedi 6 juillet 2024
    assert(f.is_weekend == 1)
    assert(f.day_period == "Afternoon")
    assert(f.is_working_hours == 1)
  }

  test("bornes des périodes de la journée") {
    assert(TimeFeatures.extract("20240701060000").get.day_period == "Morning")
    assert(TimeFeatures.extract("20240701120000").get.day_period == "Afternoon")
    assert(TimeFeatures.extract("20240701180000").get.day_period == "Evening")
    assert(TimeFeatures.extract("20240701220000").get.day_period == "Night")
  }

  test("entrées nulles, vides ou mal formées : None, sans exception") {
    assert(TimeFeatures.extract(null).isEmpty)
    assert(TimeFeatures.extract("").isEmpty)
    assert(TimeFeatures.extract("2024-07-01").isEmpty)
    assert(TimeFeatures.extract("abcdefghijklmn").isEmpty)
    assert(TimeFeatures.extract("20241399250000").isEmpty)   // 14 chiffres mais date impossible
  }
}
