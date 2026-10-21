package com.ecommerce.analytics

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Analytique business (Q4.1 et 4.2)
 *
 * On prend en entrée le DataFrame enrichi produit par DataTransformation. Colonnes utilisees :
 *   - merchant_id, merchant_name, merchant_category, merchant_region, commission_rate
 *   - user_id, amount, age_group, transaction_date
 *   - is_suspicious (pas obligatoiore)
 */
class Analytics {

  // Libelles de age_group (definis par B) -> suffixe de colonne
  private val ageGroups: Seq[(String, String)] = Seq(
    "Jeune"     -> "jeune",
    "Adulte"    -> "adulte",
    "Âge Moyen" -> "age_moyen",
    "Senior"    -> "senior",
    "Inconnu"   -> "inconnu"
  )

  /**
   * Q4.1 : une ligne par marchand
   *
   * Les jointures de Kany sont des LEFT JOIN : les transactions dont le marchand n'existe pas
   * (reference orpheline) ont merchant_name nul et on les exclut des KPI marchands.
   *
   * Le fenetres rank est calculé sur le tableau DEJA agrégé par marchand,
   * avec les chiffres non arrondis (ce n'est appliqué qu'à la fin)
   */
  def merchantReport(enriched: DataFrame): DataFrame = {
    val base = enriched.filter(col("merchant_name").isNotNull)

    val keys: Seq[Column] =
      Seq("merchant_id", "merchant_name", "merchant_category", "merchant_region", "commission_rate").map(col)

    val coreAggs: Seq[Column] = Seq(
      sum("amount").as("chiffre_affaires"),
      count(lit(1)).as("nb_transactions"),
      count_distinct(col("user_id")).as("nb_clients_uniques"),
      avg("amount").as("montant_moyen"),
      sum(col("amount") * col("commission_rate")).as("commission_totale")
    )

    // Repartition des ventes par tranche d'age : une colonne de CA par tranche, en un seul passage
    val ageAggs: Seq[Column] = ageGroups.map { case (label, slug) =>
      sum(when(col("age_group") === label, col("amount")).otherwise(lit(0.0))).as(s"ca_$slug")
    }

    // Taux de transactions suspectes
    val hasSuspicious = enriched.columns.contains("is_suspicious")
    val suspiciousAggs: Seq[Column] =
      if (hasSuspicious) Seq(avg(col("is_suspicious")).as("taux_suspects_brut")) else Seq.empty

    val aggs = coreAggs ++ ageAggs ++ suspiciousAggs
    val perMerchant = base.groupBy(keys: _*).agg(aggs.head, aggs.tail: _*)

    // Classement par CA dans la categorie et dans la region du marchand
    val byCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val byRegion   = Window.partitionBy("merchant_region").orderBy(col("chiffre_affaires").desc)
    val ranked = perMerchant
      .withColumn("rang_categorie", rank().over(byCategory))
      .withColumn("rang_region", rank().over(byRegion))

    // Arrondi a 2 decimales des montants
    val moneyCols = Seq("chiffre_affaires", "montant_moyen", "commission_totale") ++ ageGroups.map(g => s"ca_${g._2}")
    val rounded = moneyCols.foldLeft(ranked)((df, c) => df.withColumn(c, round(col(c), 2)))

    val result =
      if (hasSuspicious)
        rounded
          .withColumn("taux_transactions_suspectes", round(col("taux_suspects_brut") * 100, 2))
          .drop("taux_suspects_brut")
      else rounded

    result.orderBy(col("chiffre_affaires").desc)
  }

  /**
   * Q4.2 : analyse de cohortes
   *
   *  - cohort_month  : mois de la première transaction de l'utilisateur
   *  - period_index  : nombre de mois entre le mois de la transaction et cohort_month
   *  - taille_cohorte : nombre d'utilisateurs de la cohorte (c'est à dire ceux dont la 1ère transaction est ce mois-là)
   *  - utilisateurs_actifs / taux_retention : actifs à chaque période, et % de la cohorte initiale
   *  - ca_cohorte_periode et revenu_moyen_par_utilisateur
   */
  def cohortRetention(enriched: DataFrame): DataFrame = {
    val tx = enriched
      .filter(col("transaction_date").isNotNull && col("user_id").isNotNull)
      .select(
        col("user_id"),
        col("amount"),
        trunc(col("transaction_date"), "month").as("tx_month") // 1er jour du mois
      )

    val withCohort = tx
      .withColumn("cohort_month", min(col("tx_month")).over(Window.partitionBy("user_id")))
      .withColumn(
        "period_index",
        (year(col("tx_month")) - year(col("cohort_month"))) * 12 +
          (month(col("tx_month")) - month(col("cohort_month")))
      )

    // Taille de cohorte : chaque utilisateur a forcément une transaction en son premier mois
    val cohortSize = withCohort
      .filter(col("period_index") === 0)
      .groupBy("cohort_month")
      .agg(count_distinct(col("user_id")).as("taille_cohorte"))

    val perPeriod = withCohort
      .groupBy("cohort_month", "period_index")
      .agg(
        count_distinct(col("user_id")).as("utilisateurs_actifs"),
        sum("amount").as("ca")
      )

    perPeriod
      .join(SparkOptimizations.broadcastIfEnabled(cohortSize), Seq("cohort_month"))
      .withColumn("taux_retention", round(col("utilisateurs_actifs") * 100.0 / col("taille_cohorte"), 2))
      .withColumn("ca_cohorte_periode", round(col("ca"), 2))
      .withColumn("revenu_moyen_par_utilisateur", round(col("ca") / col("utilisateurs_actifs"), 2))
      .select(
        "cohort_month", "period_index", "taille_cohorte", "utilisateurs_actifs",
        "taux_retention", "ca_cohorte_periode", "revenu_moyen_par_utilisateur"
      )
      .orderBy("cohort_month", "period_index")
  }


  def retentionMatrix(retention: DataFrame): DataFrame =
    retention
      .groupBy("cohort_month")
      .pivot("period_index")
      .agg(first("taux_retention"))
      .orderBy("cohort_month")

  /**
   * Seules les cohortes qui ont des utilisateurs actifs a cette periode apparaissent
   * Les cohortes trop réentes (période pas encore atteinte) sont donc exclues d'office
   * En cas d'égalité, on prend la plus grande cohorte (le taux est donc plus fiable)
   */
  def bestCohortAt(retention: DataFrame, period: Int = 3): DataFrame =
    retention
      .filter(col("period_index") === period)
      .orderBy(col("taux_retention").desc, col("taille_cohorte").desc)
      .limit(1)
}
