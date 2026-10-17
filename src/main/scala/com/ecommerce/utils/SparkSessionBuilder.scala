package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def build(): SparkSession = {
    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions.toString)
      .getOrCreate()

    spark.sparkContext.setLogLevel(ConfigLoader.logLevel)
    spark
  }
}
