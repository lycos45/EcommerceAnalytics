package com.ecommerce.analytics

import java.time.{LocalDateTime, DayOfWeek}
import java.time.format.TextStyle
import java.util.Locale
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.util.Try

/** Structure renvoyée par l'UDF (Q3.1). */
case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

/** Q3.1 (Membre B) : extraction de caractéristiques temporelles depuis "yyyyMMddHHmmss". */
object TimeFeatures {

  /** Fonction pure (testable sans Spark). Renvoie None si la chaîne est nulle, vide ou mal formée. */
  def extract(s: String): Option[TimeFeatures] =
    Option(s).map(_.trim).filter(x => x.length == 14 && x.forall(_.isDigit)).flatMap { t =>
      Try {
        val dt = LocalDateTime.of(t.substring(0, 4).toInt, t.substring(4, 6).toInt, t.substring(6, 8).toInt,
          t.substring(8, 10).toInt, t.substring(10, 12).toInt, t.substring(12, 14).toInt) // lève si date impossible
        val h = dt.getHour
        val dow = dt.getDayOfWeek
        TimeFeatures(
          hour = h,
          day_of_week = dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
          day_period =
            if (h >= 6 && h < 12) "Morning"
            else if (h >= 12 && h < 18) "Afternoon"
            else if (h >= 18 && h < 22) "Evening"
            else "Night",                       // 22h-24h et 0h-6h
          is_working_hours = if (h >= 9 && h <= 17) 1 else 0
        )
      }.toOption
    }

  /** UDF Spark : struct nul si l'entrée est invalide (le job ne plante jamais). */
  val extractTimeFeatures: UserDefinedFunction = udf((s: String) => extract(s))
}
