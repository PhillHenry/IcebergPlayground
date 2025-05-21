package uk.co.odinconsultants

import org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_REST
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.polaris.PolarisRESTSetup
import uk.co.odinconsultants.polaris.PolarisRESTSetup.WAREHOUSE_NAME

import java.nio.file.Files

/** If you want to run tests outside of Maven, run:
  * docker run -i -t -p8181:8181 -v/tmp:/tmp my-polaris
  * first
  */
object SparkForTesting {
  val numThreads: Int      = 4
  val sparkCatalog: String = "spark_catalog"
  val catalog: String      = "polaris"
  val namespace: String    = "my_namespace"
  val master: String       = s"local[$numThreads]"
  val tmpDir: String       = Files.createTempDirectory("SparkForTesting").toString
  val catalog_class
      : String = // "adds support for Iceberg tables to Spark's built-in catalog, and delegates to the built-in catalog for non-Iceberg tables"
    "org.apache.iceberg.spark.SparkSessionCatalog" // not "org.apache.iceberg.spark.SparkCatalog" ?

  val sparkConf: SparkConf   = {
    PolarisRESTSetup.setup()
    println(s"Using temp directory $tmpDir")
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
      .set(
        SPARK_SESSION_EXTENSIONS.key,
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      )
      .set(s"spark.sql.catalog.${namespace}", catalog_class)
      .set(DEFAULT_CATALOG.key, catalog)
      .set(WAREHOUSE_PATH.key, tmpDir)
      .set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
      .set("spark.sql.sources.bucketing.enabled", "true")
      .set("spark.sql.sources.v2.bucketing.enabled", "true")
      .set(s"spark.sql.catalog.$catalog.uri", "http://localhost:8181/api/catalog")
      .set(s"spark.sql.catalog.$catalog.token", PolarisRESTSetup.accessToken)
      .set(s"spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set(s"spark.sql.catalog.$catalog.type", ICEBERG_CATALOG_TYPE_REST)
      .set(s"spark.sql.catalog.$catalog.warehouse", WAREHOUSE_NAME)
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      )
      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession
    .builder()
    .getOrCreate()
  spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$namespace")
  val sqlContext: SQLContext = spark.sqlContext

}
