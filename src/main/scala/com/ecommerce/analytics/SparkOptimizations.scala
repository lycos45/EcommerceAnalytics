package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


object SparkOptimizations {

  // Datasets mis en cache pour pouvoir tous les libérer en fin de pipeline
  private val tracked = mutable.ListBuffer.empty[Dataset[_]]

  private def register(ds: Dataset[_]): Unit = tracked += ds

  /**
   * Puisque le cache est paresseux, avec materialize = true,
   * on force un count() tout de suite, 
   * pour payer le calcul une seule fois et a un endroit connu
   */
  def cacheIfEnabled[T](ds: Dataset[T], materialize: Boolean = true): Dataset[T] =
    if (ConfigLoader.enableCache) {
      ds.cache()
      register(ds)
      if (materialize) ds.count()
      ds
    } else ds


  def persistLarge[T](ds: Dataset[T], materialize: Boolean = true): Dataset[T] =
    if (ConfigLoader.enableCache) {
      ds.persist(StorageLevel.MEMORY_AND_DISK_SER)
      register(ds)
      if (materialize) ds.count()
      ds
    } else ds

  def release[T](ds: Dataset[T]): Unit = {
    ds.unpersist()
    tracked.filterInPlace(_ ne ds)
  }

  def releaseAll(): Unit = {
    tracked.foreach(_.unpersist())
    tracked.clear()
  }


  def broadcastIfEnabled(df: DataFrame): DataFrame =
    if (ConfigLoader.enableBroadcast) broadcast(df) else df

  def explainJoins(df: DataFrame): Unit = df.explain()

  // Pour voir les réglages d'optimisation actifs
  def describe(): Unit = {
    println("[optimisations] cache/persist        : " + (if (ConfigLoader.enableCache) "actif" else "desactive"))
    println("[optimisations] broadcast des jointures : " + (if (ConfigLoader.enableBroadcast) "actif" else "desactive"))
    println(s"[optimisations] shuffle.partitions     : ${ConfigLoader.shufflePartitions}")
  }
}
