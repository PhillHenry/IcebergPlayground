package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._

class IcebergCRUDSpec extends AnyWordSpec {
  val IntField: String = "_1"
  "Spark" when {
    "given our configuration" should {
      val data: Seq[(Int, String)] = Seq((41, "phill"), (42, "henry"))
      val df: DataFrame            = spark.createDataFrame(data)

      "read and write to metastore" in {
        val tableName                = "spark_file_test_writeTo"
        df.show()
        df.writeTo(tableName).create()
        val output: DataFrame        = spark.read.table(tableName)
        assert(output.collect().length == data.length)
      }
    }
  }
}
