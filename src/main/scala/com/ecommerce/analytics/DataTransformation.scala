package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Q3.2, Q3.3, Q3.4 (Membre B) : enrichissement, fenêtrage, détection de comportements. */
class DataTransformation {

  // Paramètres lus dans application.conf via ConfigLoader (valeur par défaut si la clé est absente)
  private val enableBroadcast       = ConfigLoader.enableBroadcast
  private val amountExcessPct       = ConfigLoader.getDouble("app.analytics.suspicious.amount-excess-pct", 300.0)
  private val minDelayMinutes       = ConfigLoader.getInt("app.analytics.suspicious.min-delay-minutes", 5)
  private val minSuspiciousConds    = ConfigLoader.getInt("app.analytics.suspicious.min-conditions", 2)
  private val activeWindowDays      = ConfigLoader.getInt("app.analytics.active-user.window-days", 7)
  private val activeMinDistinctDays = ConfigLoader.getInt("app.analytics.active-user.min-distinct-days", 5)

  /** Applique broadcast() sur une petite table si l'option est active (Q5.2). */
  private def small(df: DataFrame): DataFrame = if (enableBroadcast) broadcast(df) else df

  /** Q3.2 : jointures + UDF + fenêtres + tranche d'âge.
   *  Choix de jointure : LEFT depuis transactions vers users / products / merchants (voir CONTRIBUTIONS.md). */
  def enrichTransactionData(transactions: Dataset[_], users: Dataset[_],
                            products: Dataset[_], merchants: Dataset[_]): DataFrame = {
    // Préfixes pour éviter toute colonne ambiguë (category, name, merchant_id existent dans plusieurs tables)
    val u = users.toDF().select(
      col("user_id"), col("age"), col("annual_income"),
      col("city").as("user_city"), col("customer_segment"),
      col("preferred_categories"), col("registration_date").as("user_registration_date"))
    val p = products.toDF().select(
      col("product_id"), col("name").as("product_name"), col("category").as("product_category"),
      col("price").as("product_price"), col("rating").as("product_rating"), col("stock").as("product_stock"))
    val m = merchants.toDF().select(
      col("merchant_id"), col("name").as("merchant_name"), col("category").as("merchant_category"),
      col("region").as("merchant_region"), col("commission_rate"),
      col("establishment_date").as("merchant_establishment_date"))

    val joined = transactions.toDF()
      .join(small(u), Seq("user_id"), "left")
      .join(small(p), Seq("product_id"), "left")
      .join(small(m), Seq("merchant_id"), "left")

    // UDF + colonnes à plat
    val withTime = joined
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .withColumn("hour", col("time_features.hour"))
      .withColumn("day_of_week", col("time_features.day_of_week"))
      .withColumn("month_name", col("time_features.month"))
      .withColumn("is_weekend", col("time_features.is_weekend"))
      .withColumn("day_period", col("time_features.day_period"))
      .withColumn("is_working_hours", col("time_features.is_working_hours"))
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("transaction_date", to_date(col("transaction_ts")))

    // Fenêtres par utilisateur
    val byUser   = Window.partitionBy("user_id")
    val byUserTs = Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))

    withTime
      .withColumn("rank_per_user", row_number().over(byUserTs))
      .withColumn("nb_transactions_user", count(lit(1)).over(byUser))
      .withColumn("age_group",
        when(col("age").isNull, "Inconnu")
          .when(col("age") <= 25, "Jeune")          // 25 ans rangé dans Jeune (trou du sujet entre "<25" et "26-44")
          .when(col("age") <= 44, "Adulte")
          .when(col("age") <= 64, "Âge Moyen")
          .otherwise("Senior"))
  }

  /** Q3.3 : montant glissant 7 jours, utilisateur actif, délai entre achats. */
  def addBehaviorWindows(enriched: DataFrame): DataFrame = {
    val win = activeWindowDays
    val secs = col("transaction_ts").cast("long")
    val dayNum = datediff(col("transaction_date"), lit("1970-01-01").cast("date"))

    // fenêtre en secondes : [t - 7 jours ; t]
    val w7sec = Window.partitionBy("user_id").orderBy(secs).rangeBetween(-win.toLong * 86400L, 0L)
    // fenêtre en numéro de jour : 7 jours calendaires glissants (jour courant inclus)
    val w7day = Window.partitionBy("user_id").orderBy(dayNum).rangeBetween(-(win - 1).toLong, 0L)
    val wLag  = Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))

    enriched
      .withColumn("_day_num", dayNum)
      .withColumn("montant_cumule_7j", sum("amount").over(w7sec))
      .withColumn("jours_distincts_7j", size(collect_set(col("_day_num")).over(w7day)))
      .withColumn("utilisateur_actif", when(col("jours_distincts_7j") >= activeMinDistinctDays, 1).otherwise(0))
      .withColumn("_prev_ts", lag("transaction_ts", 1).over(wLag))
      .withColumn("delai_jours", datediff(col("transaction_ts"), col("_prev_ts")))
      .withColumn("delai_minutes", (col("transaction_ts").cast("long") - col("_prev_ts").cast("long")) / 60.0)
      .drop("_day_num", "_prev_ts")
  }

  /** Q3.4 (BONUS) : écart au panier moyen + is_suspicious (au moins 2 conditions sur 4). */
  def detectSuspicious(df: DataFrame): DataFrame = {
    val avgAmount = avg("amount").over(Window.partitionBy("user_id"))
    val withGap = df
      .withColumn("panier_moyen_user", avgAmount)
      .withColumn("ecart_panier_moyen_pct", (col("amount") - col("panier_moyen_user")) / col("panier_moyen_user") * 100.0)

    def flag(c: Column): Column = when(c, 1).otherwise(0)   // null -> 0
    val conditions =
      flag(col("ecart_panier_moyen_pct") > amountExcessPct) +
      flag(col("day_period") === "Night") +
      flag(col("delai_minutes") < minDelayMinutes) +
      flag(col("payment_method") === "CRYPTO")

    withGap
      .withColumn("nb_conditions_suspectes", conditions)
      .withColumn("is_suspicious", when(col("nb_conditions_suspectes") >= minSuspiciousConds, 1).otherwise(0))
  }
}
