package uk.co.odinconsultants

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.rest.RESTCatalog
import org.apache.spark.sql.hive.UnsafeSpark
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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
      .set(DEFAULT_CATALOG.key, "local")
      .set(WAREHOUSE_PATH.key, tmpDir)
      .set(s"spark.sql.catalog.local.${CatalogProperties.WAREHOUSE_LOCATION}", tmpDir)
      .set(s"spark.sql.catalog.${sparkCatalog}", "org.apache.iceberg.spark.SparkCatalog") // "supports a Hive Metastore or a Hadoop warehouse as a catalog"
      .set(s"spark.sql.catalog.${sparkCatalog}.type", "hadoop") // hadoop => "Cannot use non-v1 table 'my_db.deletetripssnapshotspec' as a source", "hive" => "Table/View 'NEXT_LOCK_ID' does not exist."
      .set(s"spark.sql.catalog.${sparkCatalog}.warehouse", Files.createTempDirectory(s"hive_$sparkCatalog").toString)
      .set(s"spark.sql.catalog.${sparkCatalog}.cache-enabled", "false")
      .set(s"spark.sql.catalog.${catalog}", catalog_class)
      .set(s"spark.sql.catalog.${catalog}.type", "hive")
      .set(s"spark.sql.catalog.${catalog}.warehouse", Files.createTempDirectory("hive").toString)
      .set(s"spark.sql.catalog.${catalog}.cache-enabled", "false")

      .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.rest.type", "rest")
      .set("spark.sql.catalog.rest.uri", "http://localhost:12345")
      .set("spark.sql.catalog.rest.auth.type", "basic")
//      .set("spark.sql.catalog.rest.auth.basic.username", "your-username")
//      .set("spark.sql.catalog.rest.auth.basic.password", "your-password")
      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  UnsafeSpark.getHiveConfig().foreach(kv => sparkConf.set(kv._1, kv._2))
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession
    .builder()
//    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
//  spark.sql(s"create database $namespace") // Delegated SessionCatalog is missing. Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.
  val sqlContext: SQLContext = spark.sqlContext

}
