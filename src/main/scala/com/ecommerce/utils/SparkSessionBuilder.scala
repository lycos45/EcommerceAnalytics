package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions.toString)

    // spark-submit --master renseigne la propriete spark.master : dans ce cas on ne l'ecrase pas
    // (remarque de relecture de Bamba). Sinon (sbt run, tests) on prend celui de application.conf.
    if (!sys.props.contains("spark.master")) builder.master(ConfigLoader.sparkMaster)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(ConfigLoader.logLevel)
    spark
  }
}
