package com.ecommerce.analytics

import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

// Tests d'integration sur les vrais fichiers de src/main/resources/data.
// Les tests unitaires des regles sont dans DataValidationTest.
class DataIngestionTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark     = SparkSessionBuilder.build()
  private lazy val ingestion = new DataIngestion(spark)

  // si les donnees ne sont pas la, les tests sont annules (canceled) au lieu d'echouer
  private def requireData(): Unit = {
    val paths = Seq(ConfigLoader.transactionsPath, ConfigLoader.usersPath,
      ConfigLoader.productsPath, ConfigLoader.merchantsPath)
    assume(paths.forall(p => new File(p).exists()), s"fichiers de donnees absents : $paths")
  }

  override def afterAll(): Unit = spark.stop()

  test("les 4 fichiers sont lus avec le bon nombre de lignes") {
    requireData()
    val raw = ingestion.loadAll()
    assert(raw.merchants.count() == 600)
    assert(raw.users.count() == 12000)
    assert(raw.products.count() == 6000)
    assert(raw.transactions.count() > 130000)
  }

  test("les types sont corrects apres lecture") {
    requireData()
    val users = ingestion.readUsers()
    assert(users.schema("age").dataType.typeName == "integer")
    assert(users.schema("preferred_categories").dataType.typeName == "array")

    val merchants = ingestion.readMerchants()
    assert(merchants.schema("establishment_date").dataType.typeName == "string")
    assert(merchants.filter(_.establishment_date.length == 8).count() == merchants.count())
  }

  test("un fichier absent ne fait pas planter la lecture") {
    val res = ingestion.safeLoad("inexistant")(ingestion.readTransactions("data/nexiste_pas.csv"))
    assert(res.isEmpty)
  }

  test("la validation rejette des lignes sur chaque dataset") {
    requireData()
    val validated = new DataValidation(spark).validateAll(ingestion.loadAll())
    val taux = validated.qualityReport.collect().map(r => r.getString(0) -> r.getAs[Double]("taux_rejet")).toMap

    assert(taux.keySet == Set("transactions", "users", "products", "merchants"))
    assert(taux.values.forall(_ > 0.0), s"un taux de rejet a 0 % : $taux")
  }

  // demande winutils.exe / hadoop.dll sous Windows (voir README)
  test("le rapport de qualite est ecrit en CSV et peut etre relu") {
    import spark.implicits._
    val validation = new DataValidation(spark)
    val report = validation.buildQualityReport(Seq(("test", 10L, 8L, 2L, 1L)))

    validation.saveQualityReport(report, "target/test-output")
    val relu = spark.read.option("header", "true").csv("target/test-output/quality_report")
    assert(relu.count() == 1)
    assert(relu.select("taux_rejet").as[String].first() == "20.0")
  }
}
