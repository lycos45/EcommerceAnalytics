package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Acces centralise a application.conf.
 * Chaque getter prend une valeur par defaut utilisee si la cle est absente.
 */
object ConfigLoader {

  private lazy val config: Config = ConfigFactory.load()

  def getString(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  def getInt(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  def getDouble(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default

  def getBoolean(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default

  // Raccourcis utilises un peu partout dans le projet
  val appName: String           = getString("app.name", "EcommerceAnalytics")
  val sparkMaster: String       = getString("app.spark.master", "local[*]")
  val shufflePartitions: Int    = getInt("app.spark.shuffle.partitions", 8)
  val logLevel: String          = getString("app.spark.log-level", "WARN")

  val enableCache: Boolean      = getBoolean("app.optimization.enable-cache", true)
  val enableBroadcast: Boolean  = getBoolean("app.optimization.enable-broadcast", true)

  val transactionsPath: String  = getString("app.data.input.transactions", "data/transactions.csv")
  val usersPath: String         = getString("app.data.input.users", "data/users.json")
  val productsPath: String      = getString("app.data.input.products", "data/products.parquet")
  val merchantsPath: String     = getString("app.data.input.merchants", "data/merchants.csv")
  val outputPath: String        = getString("app.data.output.path", "output/")
}
