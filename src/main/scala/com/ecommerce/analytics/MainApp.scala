package com.ecommerce.analytics

import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.NonFatal


object MainApp {

  def main(args: Array[String]): Unit = {
    var spark: Option[SparkSession] = None
    var failed = false

    try {
      spark = Some(SparkSessionBuilder.build())
      run(spark.get)
    } catch {
      case NonFatal(e) =>
        failed = true
        println(s"\n[MainApp] ERREUR : le pipeline a echoue (${e.getClass.getSimpleName}) : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      SparkOptimizations.releaseAll()
      spark.foreach(_.stop())
    }

    if (failed) sys.exit(1)
  }

  private def run(spark: SparkSession): Unit = {
    val out = ConfigLoader.outputPath.stripSuffix("/")
    SparkOptimizations.describe()

    val raw = timed("ingestion") {
      new DataIngestion(spark).loadAll()
    }

    val validation = new DataValidation(spark)
    val validated = timed("validation") {
      val v = validation.validateAll(raw)
      validation.saveQualityReport(v.qualityReport)
      v
    }

    val enriched = timed("transformation") {
      // Les transactions sont la plus grosse table : stockage sérialisé, avec débordement sur disque
      val transactions = SparkOptimizations.persistLarge(validated.transactions.valid)

      val t = new DataTransformation()
      val base = t.enrichTransactionData(
        transactions,
        validated.users.valid,
        validated.products.valid,
        validated.merchants.valid
      )
      val full = t.detectSuspicious(t.addBehaviorWindows(base))

      // Le DataFrame enrichi est lu par les KPI, les cohortes et l'écriture donc on le met en cach
      SparkOptimizations.cacheIfEnabled(full)
    }

    val analytics = new Analytics()
    val merchants = analytics.merchantReport(enriched)
    val retention = analytics.cohortRetention(enriched)
    val matrix    = analytics.retentionMatrix(retention)
    val best      = analytics.bestCohortAt(retention, 3)

    timed("analytique") {
      println("\n===== KPI par marchand (20 premiers par chiffre d'affaires) =====")
      merchants.show(20, truncate = false)

      println("\n===== Rétention par cohorte (taux en %) =====")
      retention.show(50, truncate = false)

      println("\n===== Matrice de rétention (lignes = cohortes, colonnes = périodes en mois) =====")
      matrix.show(50, truncate = false)

      println("\n===== Meilleure cohorte a 3 mois =====")
      best.show(truncate = false)
    }

    timed("ecriture") {
      save(merchants, "kpi_marchands", out)
      save(retention, "retention_cohortes", out)
      save(matrix, "matrice_retention", out)
      save(best, "meilleure_cohorte_3_mois", out)

      val rejected = Seq(
        "transactions" -> validated.transactions.rejected,
        "users"        -> validated.users.rejected,
        "products"     -> validated.products.rejected,
        "merchants"    -> validated.merchants.rejected
      )
      rejected.foreach { case (name, df) =>
        df.write.mode("overwrite").parquet(s"$out/rejected/$name")
      }
    }

    println(s"\n[MainApp] Pipeline terminée. Resultats dans : $out")
  }

  private def save(df: DataFrame, name: String, out: String): Unit = {
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$out/csv/$name")
    df.write.mode("overwrite").parquet(s"$out/parquet/$name")
    println(s"[ecriture] $name -> $out/csv/$name et $out/parquet/$name")
  }

  // mesure et affiche la duree d'une étape
  private def timed[T](step: String)(body: => T): T = {
    val start = System.nanoTime()
    println(s"\n[$step] debut")
    try body
    finally println(f"[$step] fin en ${(System.nanoTime() - start) / 1e9}%.1f s")
  }
}
