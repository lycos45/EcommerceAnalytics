package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

// Une ligne du DataFrame enrichi, limitée aux colonnes utilisees par Analytics
case class AnalyticsTestRow(
  merchant_id: String,
  merchant_name: String,
  merchant_category: String,
  merchant_region: String,
  commission_rate: Double,
  user_id: String,
  amount: Double,
  age_group: String,
  transaction_date: String,
  is_suspicious: Int
)

class AnalyticsTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .appName("AnalyticsTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private lazy val analytics = new Analytics()

  override def afterAll(): Unit = spark.stop()

  private def toEnriched(rows: Seq[AnalyticsTestRow]): DataFrame = {
    import spark.implicits._
    rows.toDF().withColumn("transaction_date", to_date(col("transaction_date")))
  }

  private def row(mid: String, name: String, cat: String, region: String, rate: Double,
                  user: String, amount: Double, age: String, date: String, susp: Int = 0) =
    AnalyticsTestRow(mid, name, cat, region, rate, user, amount, age, date, susp)

  // Q4.1 : KPI marchands

  private lazy val merchantRows = Seq(
    // M1 : 3 transactions, 2 clients, CA 200, commission 10 %
    row("M1", "Shop1", "Electronics", "R1", 0.10, "U1", 100.0, "Jeune", "2024-01-10", susp = 1),
    row("M1", "Shop1", "Electronics", "R1", 0.10, "U1", 50.0, "Jeune", "2024-01-11"),
    row("M1", "Shop1", "Electronics", "R1", 0.10, "U2", 50.0, "Adulte", "2024-01-12"),
    // M2 : même catégorie et meme region que M1, mais plus gros CA
    row("M2", "Shop2", "Electronics", "R1", 0.20, "U3", 300.0, "Senior", "2024-01-13"),
    // M3 : seul dans sa catégorie et sa région
    row("M3", "Shop3", "Books", "R2", 0.05, "U1", 80.0, "Jeune", "2024-01-14"),
    // Transaction orpheline (marchand inconnu, merchant_name nul) : doit être ignorée
    row("M9", null, null, null, 0.0, "U4", 999.0, "Inconnu", "2024-01-15")
  )

  test("merchantReport : une ligne par marchand les orphelins sont exclus") {
    val report = analytics.merchantReport(toEnriched(merchantRows))
    val ids = report.select("merchant_id").collect().map(_.getString(0)).toSet
    assert(ids == Set("M1", "M2", "M3"))
  }

  test("merchantReport : CA, nb transactions, clients uniques, moyenne, commission") {
    val report = analytics.merchantReport(toEnriched(merchantRows))
    val m1 = report.filter(col("merchant_id") === "M1").first()

    assert(m1.getAs[Double]("chiffre_affaires") == 200.0)
    assert(m1.getAs[Long]("nb_transactions") == 3L)
    assert(m1.getAs[Long]("nb_clients_uniques") == 2L)
    assert(m1.getAs[Double]("montant_moyen") == 66.67)
    assert(m1.getAs[Double]("commission_totale") == 20.0)
  }

  test("merchantReport : repartition du CA par tranche d'âge") {
    val report = analytics.merchantReport(toEnriched(merchantRows))
    val m1 = report.filter(col("merchant_id") === "M1").first()

    assert(m1.getAs[Double]("ca_jeune") == 150.0)
    assert(m1.getAs[Double]("ca_adulte") == 50.0)
    assert(m1.getAs[Double]("ca_senior") == 0.0)
  }

  test("merchantReport : classement dans la catéorie et dans la région") {
    val report = analytics.merchantReport(toEnriched(merchantRows))
    def rangs(id: String): (Int, Int) = {
      val r = report.filter(col("merchant_id") === id).first()
      (r.getAs[Int]("rang_categorie"), r.getAs[Int]("rang_region"))
    }

    assert(rangs("M2") == ((1, 1))) // 300 > 200 dans Electronics / R1
    assert(rangs("M1") == ((2, 2)))
    assert(rangs("M3") == ((1, 1))) // seul dans Books / R2
  }

  test("merchantReport : taux de transactions suspectes en %") {
    val report = analytics.merchantReport(toEnriched(merchantRows))
    val m1 = report.filter(col("merchant_id") === "M1").first()
    // 1 transaction suspecte sur 3
    assert(m1.getAs[Double]("taux_transactions_suspectes") == 33.33)
  }

  // Q4.2 : cohortes

  private lazy val cohortRows = {
    def r(user: String, date: String) = row("M1", "Shop1", "Electronics", "R1", 0.1, user, 10.0, "Adulte", date)
    Seq(
      // Cohorte de janvier 2024 : U1 (revient en fev. et en avr.), U2 (ne revient pas)
      r("U1", "2024-01-10"), r("U1", "2024-02-05"), r("U1", "2024-04-20"),
      r("U2", "2024-01-20"),
      // Cohorte de fevrier 2024 : U3 et U4 reviennent tous les deux en période 3 (mai)
      r("U3", "2024-02-15"), r("U3", "2024-05-02"),
      r("U4", "2024-02-20"), r("U4", "2024-05-25")
    )
  }

  private def rate(retention: DataFrame, cohort: String, period: Int): Double =
    retention
      .filter(col("cohort_month").cast("string") === cohort && col("period_index") === period)
      .first()
      .getAs[Double]("taux_retention")

  test("cohortRetention : taille de cohorte et période 0 a 100 %") {
    val retention = analytics.cohortRetention(toEnriched(cohortRows))

    val jan = retention.filter(col("cohort_month").cast("string") === "2024-01-01" && col("period_index") === 0).first()
    assert(jan.getAs[Long]("taille_cohorte") == 2L)
    assert(jan.getAs[Long]("utilisateurs_actifs") == 2L)
    assert(rate(retention, "2024-01-01", 0) == 100.0)
  }

  test("cohortRetention : taux de rétention par période") {
    val retention = analytics.cohortRetention(toEnriched(cohortRows))

    assert(rate(retention, "2024-01-01", 1) == 50.0)  // seul U1 revient en février
    assert(rate(retention, "2024-01-01", 3) == 50.0)  // seul U1 revient en avril
    assert(rate(retention, "2024-02-01", 3) == 100.0) // U3 et U4 reviennent en mai
  }

  test("cohortRetention : CA et revenu moyen par utilisateur") {
    val retention = analytics.cohortRetention(toEnriched(cohortRows))
    val row = retention
      .filter(col("cohort_month").cast("string") === "2024-02-01" && col("period_index") === 3)
      .first()

    assert(row.getAs[Double]("ca_cohorte_periode") == 20.0) // 2 transactions de 10
    assert(row.getAs[Double]("revenu_moyen_par_utilisateur") == 10.0)
  }

  test("bestCohortAt : la meilleure rétention à 3 mois est la cohorte de février") {
    val retention = analytics.cohortRetention(toEnriched(cohortRows))
    val best = analytics.bestCohortAt(retention, 3).collect()

    assert(best.length == 1)
    assert(best.head.getAs[java.sql.Date]("cohort_month").toString == "2024-02-01")
    assert(best.head.getAs[Double]("taux_retention") == 100.0)
  }

  test("retentionMatrix : une ligne par cohorte") {
    val retention = analytics.cohortRetention(toEnriched(cohortRows))
    val matrix = analytics.retentionMatrix(retention)

    assert(matrix.count() == 2L)
    assert(matrix.columns.contains("0") && matrix.columns.contains("3"))
  }
}
