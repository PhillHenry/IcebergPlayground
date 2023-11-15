package uk.co.odinconsultants

import org.apache.iceberg.CatalogProperties
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{
  CATALOG_IMPLEMENTATION,
  SPARK_SESSION_EXTENSIONS,
  WAREHOUSE_PATH,
}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Files

object SparkForTesting {
  val master   : String    = "local[2]"
  val tmpDir   : String    = Files.createTempDirectory("SparkForTesting").toString
  val sparkConf: SparkConf = {
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
      .set(
        SPARK_SESSION_EXTENSIONS.key,
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      )
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set(DEFAULT_CATALOG.key, "local")
      .set(WAREHOUSE_PATH.key, tmpDir)
      .set(s"spark.sql.catalog.local.${CatalogProperties.WAREHOUSE_LOCATION}", tmpDir)
//      .set("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
//      .set("spark.sql.catalog.hive_prod.type", "hive")
//      .set("spark.sql.catalog.hive_prod.uri", "thrift://localhost:10000")
      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

}
