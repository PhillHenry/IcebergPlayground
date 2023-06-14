package uk.co.odinconsultants

import org.apache.iceberg.CatalogProperties
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Files

object SparkForTesting {
  val master: String         = "local[*]"
  val sparkConf: SparkConf   = {
    val dir: String = Files.createTempDirectory("SparkForTesting").toString
    println(s"Using temp directory $dir")
    System.setProperty("derby.system.home", dir)
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
//      .set("spark.driver.allowMultipleContexts", "true")
      .set(SPARK_SESSION_EXTENSIONS.key, "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set(DEFAULT_CATALOG.key, "local")
      .set(WAREHOUSE_PATH.key, dir)
      .set(s"spark.sql.catalog.local.${CatalogProperties.WAREHOUSE_LOCATION}", dir)
      .setSparkHome(dir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

}
