package uk.co.odinconsultants

import org.apache.iceberg.CatalogProperties
import org.apache.spark.sql.hive.UnsafeSpark
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Files

object SparkForTesting {
  val catalog: String        = "spark_catalog"
  val database: String       = "database"
  val namespace: String      = s"${catalog}.$database"
  val master: String         = "local[2]"
  val tmpDir: String         = Files.createTempDirectory("SparkForTesting").toString
  val catalog_class: String  =
    "org.apache.iceberg.spark.SparkSessionCatalog" // not "org.apache.iceberg.spark.SparkCatalog" ?
  val sparkConf: SparkConf   = {
    println(s"Using temp directory $tmpDir")
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
      .set(
        SPARK_SESSION_EXTENSIONS.key,
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      )
      .set(s"spark.sql.catalog.${namespace}", catalog_class)
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set(DEFAULT_CATALOG.key, "local")
      .set(WAREHOUSE_PATH.key, tmpDir)
      .set(s"spark.sql.catalog.local.${CatalogProperties.WAREHOUSE_LOCATION}", tmpDir)
      .set(s"spark.sql.catalog.${catalog}", catalog_class)
      .set(s"spark.sql.catalog.${catalog}.type", "hive")
      .set(s"spark.sql.catalog.${catalog}.warehouse", Files.createTempDirectory("hive").toString)
      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  UnsafeSpark.getHiveConfig().foreach(kv => sparkConf.set(kv._1, kv._2))
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession
    .builder()
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
  spark.sql(s"create database $namespace")
  val sqlContext: SQLContext = spark.sqlContext

}
