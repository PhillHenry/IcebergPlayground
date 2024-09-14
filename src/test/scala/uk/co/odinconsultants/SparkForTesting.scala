package uk.co.odinconsultants

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.rest.RESTCatalog
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_REST

import java.nio.file.Files
import scala.jdk.CollectionConverters._

object SparkForTesting {
  //System.getProperties().setProperty("derby.system.home", Files.createTempDirectory("derby").toString);
  val sparkCatalog: String = "spark_catalog"
  val catalog: String        = "my_spark_catalog"
  val database: String       = "my_database"
  val namespace: String      = s"${catalog}.$database"
  val master: String         = "local[2]"
  val tmpDir: String         = Files.createTempDirectory("SparkForTesting").toString
  val catalog_class: String  = // "adds support for Iceberg tables to Spark's built-in catalog, and delegates to the built-in catalog for non-Iceberg tables"
    "org.apache.iceberg.spark.SparkSessionCatalog" // not "org.apache.iceberg.spark.SparkCatalog" ?

//  val restCatalog = new RESTCatalog()
//  restCatalog.initialize("rest_catalog", Map(CatalogProperties.URI -> "http://localhost:12345").asJava)

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
//      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set(DEFAULT_CATALOG.key, "polaris")
      .set(WAREHOUSE_PATH.key, tmpDir)
      .set("spark.sql.catalog.polaris.uri", "http://localhost:8181/api/catalog")
      .set("spark.sql.catalog.polaris.token", "principal:root;realm:default-realm")
      .set("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.polaris.type", ICEBERG_CATALOG_TYPE_REST)
      .set("spark.sql.catalog.polaris.warehouse", "manual_spark")
      .set("spark.sql.defaultCatalog", "polaris")
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession
    .builder()
    .getOrCreate()
//  spark.sql(s"create database $namespace") // Delegated SessionCatalog is missing. Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.
  val sqlContext: SQLContext = spark.sqlContext

}
