package com.ecommerce.analytics

import com.ecommerce.utils.SparkSessionBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

// Tests sur les vrais fichiers de src/main/resources/data
class DataIngestionTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark     = SparkSessionBuilder.build()
  private lazy val ingestion = new DataIngestion(spark)

  override def afterAll(): Unit = spark.stop()

  test("les 4 fichiers sont lus avec le bon nombre de lignes") {
    val raw = ingestion.loadAll()
    assert(raw.merchants.count() == 600)
    assert(raw.users.count() == 12000)
    assert(raw.products.count() == 6000)
    assert(raw.transactions.count() > 130000)
  }

  test("les types sont corrects apres lecture") {
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
    val validated = new DataValidation(spark).validateAll(ingestion.loadAll())
    val taux = validated.qualityReport.collect().map(r => r.getString(0) -> r.getAs[Double]("taux_rejet")).toMap

    assert(taux.keySet == Set("transactions", "users", "products", "merchants"))
    assert(taux.values.forall(_ > 0.0), s"un taux de rejet a 0 % : $taux")

    // le rapport doit pouvoir etre relu depuis le CSV ecrit
    new DataValidation(spark).saveQualityReport(validated.qualityReport, "target/test-output")
    val relu = spark.read.option("header", "true").csv("target/test-output/quality_report")
    assert(relu.count() == 4)
  }
}
