package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}

// Les 4 jeux de donnees bruts, tels que lus sur disque (avant validation)
case class RawData(
  transactions: Dataset[Transaction],
  users: Dataset[User],
  products: Dataset[Product],
  merchants: Dataset[Merchant]
)

class DataIngestion(spark: SparkSession) {

  import spark.implicits._

  // Schema explicite demande pour les transactions.
  // timestamp reste une String : une partie des valeurs est mal formee, on les filtre a la validation.
  val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType),
    StructField("user_id", StringType),
    StructField("product_id", StringType),
    StructField("merchant_id", StringType),
    StructField("amount", DoubleType),
    StructField("timestamp", StringType),
    StructField("location", StringType),
    StructField("payment_method", StringType),
    StructField("category", StringType)
  ))

  // Pour le JSON on fixe aussi le schema : preferred_categories est un tableau imbrique
  // et Spark infere age en Long alors que la case class attend un Int.
  val userSchema: StructType = StructType(Seq(
    StructField("user_id", StringType),
    StructField("age", IntegerType),
    StructField("annual_income", DoubleType),
    StructField("city", StringType),
    StructField("customer_segment", StringType),
    StructField("preferred_categories", ArrayType(StringType)),
    StructField("registration_date", StringType)
  ))

  def readTransactions(path: String = ConfigLoader.transactionsPath): Dataset[Transaction] =
    spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(path)
      .as[Transaction]

  def readUsers(path: String = ConfigLoader.usersPath): Dataset[User] =
    spark.read
      .schema(userSchema)
      .json(path)
      .as[User]

  def readProducts(path: String = ConfigLoader.productsPath): Dataset[Product] =
    spark.read
      .parquet(path)
      .as[Product]

  def readMerchants(path: String = ConfigLoader.merchantsPath): Dataset[Merchant] = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    // L'inference lit establishment_date (yyyyMMdd) comme un entier, on le repasse en texte
    df.withColumn("establishment_date", col("establishment_date").cast(StringType))
      .withColumn("commission_rate", col("commission_rate").cast(DoubleType))
      .as[Merchant]
  }

  /**
   * Lit un dataset en capturant les erreurs (fichier absent, colonnes manquantes...).
   * Affiche le nombre de lignes lues. Renvoie None si la lecture a echoue.
   */
  def safeLoad[T](name: String)(read: => Dataset[T]): Option[Dataset[T]] =
    try {
      val ds = read
      val nb = ds.count()
      println(f"[ingestion] $name%-13s : $nb%d lignes lues")
      Some(ds)
    } catch {
      case e: AnalysisException =>
        println(s"[ingestion] ERREUR $name : fichier introuvable ou structure incorrecte")
        println(s"            ${e.getMessage.linesIterator.next()}")
        None
      case e: Exception =>
        println(s"[ingestion] ERREUR $name : ${e.getClass.getSimpleName} - ${e.getMessage}")
        None
    }

  // Charge les 4 sources. Si l'une d'elles manque, on arrete : la suite du pipeline en a besoin.
  def loadAll(): RawData = {
    val transactions = safeLoad("transactions")(readTransactions())
    val users        = safeLoad("users")(readUsers())
    val products     = safeLoad("products")(readProducts())
    val merchants    = safeLoad("merchants")(readMerchants())

    (transactions, users, products, merchants) match {
      case (Some(t), Some(u), Some(p), Some(m)) => RawData(t, u, p, m)
      case _ =>
        throw new IllegalStateException("Ingestion incomplete : au moins un fichier n'a pas pu etre lu")
    }
  }
}
