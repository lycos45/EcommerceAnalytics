package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SparkOptimizationsTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .appName("SparkOptimizationsTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit = {
    SparkOptimizations.releaseAll()
    spark.stop()
  }

  test("cacheIfEnabled : le DataFrame est en cache si l'option est active, puis libere par release") {
    val df = spark.range(100).toDF("id")
    val cached = SparkOptimizations.cacheIfEnabled(df)

    if (ConfigLoader.enableCache) assert(cached.storageLevel != StorageLevel.NONE)
    else assert(cached.storageLevel == StorageLevel.NONE)

    SparkOptimizations.release(cached)
    assert(cached.storageLevel == StorageLevel.NONE)
  }

  test("persistLarge : niveau MEMORY_AND_DISK_SER si l'option est active") {
    val df = spark.range(100).toDF("id")
    val persisted = SparkOptimizations.persistLarge(df)

    if (ConfigLoader.enableCache) assert(persisted.storageLevel == StorageLevel.MEMORY_AND_DISK_SER)
    else assert(persisted.storageLevel == StorageLevel.NONE)

    SparkOptimizations.release(persisted)
  }

  test("releaseAll : libere tous les Datasets mis en cache") {
    val a = SparkOptimizations.cacheIfEnabled(spark.range(10).toDF("id"))
    val b = SparkOptimizations.persistLarge(spark.range(20).toDF("id"))

    SparkOptimizations.releaseAll()

    assert(a.storageLevel == StorageLevel.NONE)
    assert(b.storageLevel == StorageLevel.NONE)
  }

  test("broadcastIfEnabled : ne change pas le contenu du DataFrame") {
    val df = spark.range(10).toDF("id")
    assert(SparkOptimizations.broadcastIfEnabled(df).count() == 10L)
  }
}
