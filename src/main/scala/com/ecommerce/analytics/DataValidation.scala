package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}

// Resultat d'une validation : les lignes gardees et les lignes rejetees (avec la raison)
case class ValidationResult[T](valid: Dataset[T], rejected: DataFrame)

case class ValidatedData(
  transactions: ValidationResult[Transaction],
  users: ValidationResult[User],
  products: ValidationResult[Product],
  merchants: ValidationResult[Merchant],
  qualityReport: DataFrame
)

class DataValidation(spark: SparkSession) {

  import spark.implicits._

  // Seuils lus dans application.conf
  private val minAmount       = ConfigLoader.getDouble("app.validation.transaction.min-amount", 0.0)
  private val timestampLength = ConfigLoader.getInt("app.validation.transaction.timestamp-length", 14)
  private val minAge          = ConfigLoader.getInt("app.validation.user.min-age", 16)
  private val maxAge          = ConfigLoader.getInt("app.validation.user.max-age", 100)
  private val minIncome       = ConfigLoader.getDouble("app.validation.user.min-income", 0.0)
  private val minPrice        = ConfigLoader.getDouble("app.validation.product.min-price", 0.0)
  private val minRating       = ConfigLoader.getDouble("app.validation.product.min-rating", 1.0)
  private val maxRating       = ConfigLoader.getDouble("app.validation.product.max-rating", 5.0)
  private val minCommission   = ConfigLoader.getDouble("app.validation.merchant.min-commission", 0.0)
  private val maxCommission   = ConfigLoader.getDouble("app.validation.merchant.max-commission", 1.0)

  /*
   * Chaque regle produit un libelle quand elle est violee, null sinon.
   * concat_ws ignore les null : on obtient la liste des regles violees,
   * ou une chaine vide si la ligne est correcte.
   */
  private def splitValid[T: Encoder](ds: Dataset[T], rules: Seq[Column]): ValidationResult[T] = {
    val tagged = ds.withColumn("rejection_reason", concat_ws(" ; ", rules: _*))

    val valid    = tagged.filter(col("rejection_reason") === "").drop("rejection_reason").as[T]
    val rejected = tagged.filter(col("rejection_reason") =!= "")
    ValidationResult(valid, rejected)
  }

  def validateTransactions(ds: Dataset[Transaction]): ValidationResult[Transaction] =
    splitValid(ds, Seq(
      when(col("amount").isNull, lit("amount manquant")),
      when(col("amount") <= minAmount, lit(s"amount <= $minAmount")),
      when(col("timestamp").isNull, lit("timestamp manquant")),
      when(length(col("timestamp")) =!= timestampLength, lit(s"timestamp different de $timestampLength caracteres"))
    ))

  def validateUsers(ds: Dataset[User]): ValidationResult[User] =
    splitValid(ds, Seq(
      when(col("age").isNull, lit("age manquant")),
      when(col("age") < minAge || col("age") > maxAge, lit(s"age hors [$minAge, $maxAge]")),
      when(col("annual_income").isNull, lit("annual_income manquant")),
      when(col("annual_income") <= minIncome, lit(s"annual_income <= $minIncome"))
    ))

  def validateProducts(ds: Dataset[Product]): ValidationResult[Product] =
    splitValid(ds, Seq(
      when(col("price").isNull, lit("price manquant")),
      when(col("price") <= minPrice, lit(s"price <= $minPrice")),
      when(col("rating").isNull, lit("rating manquant")),
      when(col("rating") < minRating || col("rating") > maxRating, lit(s"rating hors [$minRating, $maxRating]"))
    ))

  def validateMerchants(ds: Dataset[Merchant]): ValidationResult[Merchant] =
    splitValid(ds, Seq(
      when(col("commission_rate").isNull, lit("commission_rate manquant")),
      when(col("commission_rate") < minCommission || col("commission_rate") > maxCommission,
        lit(s"commission_rate hors [$minCommission, $maxCommission]"))
    ))

  // Nombre total de valeurs nulles du dataset, toutes colonnes confondues (un seul passage)
  def countNulls(df: DataFrame): Long = {
    val perColumn = df.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)))
    val total     = perColumn.reduce(_ + _)
    Option(df.agg(total).first().get(0)).map(_.toString.toLong).getOrElse(0L)
  }

  // Une ligne du rapport de qualite pour un dataset
  private def reportLine(name: String, raw: DataFrame, result: ValidationResult[_]): (String, Long, Long, Long, Long) = {
    val nbRead     = raw.count()
    val nbValid    = result.valid.count()
    val nbRejected = result.rejected.count()
    println(f"[validation] $name%-13s : $nbValid%d lignes valides / $nbRead%d ($nbRejected%d rejetees)")
    (name, nbRead, nbValid, nbRejected, countNulls(raw))
  }

  def buildQualityReport(lines: Seq[(String, Long, Long, Long, Long)]): DataFrame =
    lines.toDF("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "nb_valeurs_nulles")
      .withColumn("taux_rejet",
        when(col("nb_lignes_lues") > 0, round(col("nb_lignes_rejetees") * 100.0 / col("nb_lignes_lues"), 2))
          .otherwise(lit(0.0)))
      .select("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "taux_rejet", "nb_valeurs_nulles")

  def saveQualityReport(report: DataFrame, outputPath: String = ConfigLoader.outputPath): Unit = {
    val target = s"${outputPath.stripSuffix("/")}/quality_report"
    report.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(target)
    println(s"[validation] rapport de qualite ecrit dans $target")
  }

  // Valide les 4 datasets, affiche les compteurs et produit le rapport de qualite
  def validateAll(raw: RawData): ValidatedData = {
    val transactions = validateTransactions(raw.transactions)
    val users        = validateUsers(raw.users)
    val products     = validateProducts(raw.products)
    val merchants    = validateMerchants(raw.merchants)

    val report = buildQualityReport(Seq(
      reportLine("transactions", raw.transactions.toDF(), transactions),
      reportLine("users", raw.users.toDF(), users),
      reportLine("products", raw.products.toDF(), products),
      reportLine("merchants", raw.merchants.toDF(), merchants)
    ))

    println("\n===== Rapport de qualite des donnees =====")
    report.show(truncate = false)

    ValidatedData(transactions, users, products, merchants, report)
  }
}
