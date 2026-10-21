package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DataValidationTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .appName("DataValidationTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private lazy val validation = new DataValidation(spark)

  override def afterAll(): Unit = spark.stop()

  private def tx(id: String, amount: Option[Double], ts: String) =
    Transaction(id, "U1", "P1", "M1", amount, ts, "Paris", "CARD", "Books")

  test("transactions : montant <= 0, nul ou timestamp mal forme sont rejetes") {
    import spark.implicits._
    val ds = Seq(
      tx("ok", Some(10.5), "20240701021822"),
      tx("negatif", Some(-3.0), "20240701021822"),
      tx("zero", Some(0.0), "20240701021822"),
      tx("sans_montant", None, "20240701021822"),
      tx("ts_court", Some(5.0), "20240701"),
      tx("ts_null", Some(5.0), null),
      tx("ts_impossible", Some(5.0), "20241399250000")
    ).toDS()

    val res = validation.validateTransactions(ds)

    assert(res.valid.map(_.transaction_id).collect().toSeq == Seq("ok"))
    assert(res.rejected.count() == 6)
    assert(res.rejected.columns.contains("rejection_reason"))

    val reason = res.rejected.filter($"transaction_id" === "ts_court").select("rejection_reason").as[String].first()
    assert(reason.contains("timestamp"))
  }

  test("users : age entre 16 et 100 et revenu > 0") {
    import spark.implicits._
    def u(id: String, age: Option[Int], income: Option[Double]) =
      User(id, age, income, "Lyon", "Standard", Seq("Books"), "20230101")

    val ds = Seq(
      u("ok", Some(30), Some(25000.0)),
      u("bornes", Some(16), Some(1.0)),
      u("trop_jeune", Some(12), Some(20000.0)),
      u("trop_vieux", Some(140), Some(20000.0)),
      u("revenu_negatif", Some(40), Some(-1500.0)),
      u("age_null", None, Some(20000.0))
    ).toDS()

    val res = validation.validateUsers(ds)
    assert(res.valid.map(_.user_id).collect().toSet == Set("ok", "bornes"))
    assert(res.rejected.count() == 4)
  }

  test("products : prix > 0 et note entre 1 et 5") {
    import spark.implicits._
    def p(id: String, price: Option[Double], rating: Option[Double]) =
      Product(id, "nom", "Books", price, "M1", rating, Some(10))

    val ds = Seq(
      p("ok", Some(9.99), Some(4.5)),
      p("prix_zero", Some(0.0), Some(3.0)),
      p("note_haute", Some(10.0), Some(6.4)),
      p("note_negative", Some(10.0), Some(-1.0))
    ).toDS()

    val res = validation.validateProducts(ds)
    assert(res.valid.map(_.product_id).collect().toSeq == Seq("ok"))
    assert(res.rejected.count() == 3)
  }

  test("merchants : commission comprise entre 0 et 1") {
    import spark.implicits._
    def m(id: String, rate: Option[Double]) = Merchant(id, "Shop", "Books", "Bretagne", rate, "20220101")

    val ds = Seq(m("ok", Some(0.06)), m("zero", Some(0.0)), m("neg", Some(-0.05)), m("trop", Some(2.0))).toDS()

    val res = validation.validateMerchants(ds)
    assert(res.valid.map(_.merchant_id).collect().toSet == Set("ok", "zero"))
    assert(res.rejected.count() == 2)
  }

  test("plusieurs regles violees : toutes apparaissent dans rejection_reason") {
    import spark.implicits._
    val res = validation.validateTransactions(Seq(tx("double", Some(-1.0), "123")).toDS())
    val reason = res.rejected.select("rejection_reason").as[String].first()
    assert(reason.contains("amount") && reason.contains("timestamp"))
  }

  test("rapport de qualite : taux de rejet arrondi a 2 decimales") {
    val report = validation.buildQualityReport(Seq(("test", 3L, 2L, 1L, 0L)))
    val row = report.first()
    assert(row.getAs[Double]("taux_rejet") == 33.33)
    assert(report.columns.toSeq == Seq(
      "dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "taux_rejet", "nb_valeurs_nulles"))
  }

  test("references orphelines : transactions vers un user, produit ou marchand inexistant") {
    import spark.implicits._
    def t(id: String, u: String, p: String, m: String) =
      Transaction(id, u, p, m, Some(10.0), "20240701021822", "Paris", "CARD", "Books")

    val raw = RawData(
      transactions = Seq(
        t("ok", "U1", "P1", "M1"),
        t("user_inconnu", "U9", "P1", "M1"),
        t("produit_inconnu", "U1", "P9", "M1"),
        t("tout_inconnu", "U9", "P9", "M9")
      ).toDS(),
      users = Seq(User("U1", Some(30), Some(1000.0), "Lyon", "Standard", Seq("Books"), "20230101")).toDS(),
      products = Seq(Product("P1", "nom", "Books", Some(5.0), "M1", Some(4.0), Some(1))).toDS(),
      merchants = Seq(Merchant("M1", "Shop", "Books", "Bretagne", Some(0.1), "20220101")).toDS()
    )

    assert(validation.countOrphans(raw) == OrphanCounts(users = 2, products = 2, merchants = 1))

    val report = validation.buildQualityReport(
      Seq(("transactions", 4L, 4L, 0L, 0L), ("users", 1L, 1L, 0L, 0L)), Some(OrphanCounts(2, 2, 1)))
    val parDataset = report.collect().map(r => r.getString(0) -> r.getAs[Any]("nb_user_id_orphelins")).toMap
    assert(parDataset("transactions") == 2L)
    assert(parDataset("users") == null)
  }

  test("countNulls compte les nulls de toutes les colonnes") {
    import spark.implicits._
    val df = Seq[(String, Option[Int])](("a", Some(1)), (null, None), ("c", None)).toDF("x", "y")
    assert(validation.countNulls(df) == 3)
  }
}
