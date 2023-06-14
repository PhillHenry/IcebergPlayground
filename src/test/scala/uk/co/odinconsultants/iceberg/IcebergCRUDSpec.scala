package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._

class IcebergCRUDSpec extends AnyWordSpec {
  val IntField: String = "_1"
  "Spark" when {
    "given a dataset to CRUD" should {
      val data: Seq[(Int, String)] = Seq((41, "phill"), (42, "henry"))
      val df: DataFrame            = spark.createDataFrame(data)
      val tableName                = "spark_file_test_writeTo"

      "create the appropriate Iceberg files" in {
        df.writeTo(tableName).create()
        val output: DataFrame = spark.read.table(tableName)
        assert(output.collect().length == data.length)
      }

      "support updates" in {
        val newVal = "ipse locum"
        spark.sqlContext.sql(s"update $tableName set _2='$newVal'")
        val output: DataFrame = spark.read.table(tableName)
        assert(output.collect().length == data.length)
        for {
          row <- output.collect()
        } yield {
          assert(row.getString(1) == newVal)
        }
      }
    }
  }
}
