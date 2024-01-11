package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

import scala.util.Random

class ZOrderingSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  info("https://iceberg.apache.org/docs/1.4.2/spark-procedures/")

  s"A table that has many files" should {
    s"have those files aggregated" in new SimpleSparkFixture {
      def parquetFiles(): Seq[String] = dataFilesIn(tableName).filter(_.endsWith(".parquet"))
      override def num_rows: Int = 20000

      val shuffled = Random.shuffle(data)
      Given(s"$num_rows rows of data that look like\n${prettyPrintSampleOf(shuffled)}\nare initially written to table '$tableName'")
      spark.createDataFrame(shuffled).writeTo(tableName).create()
      val filesBefore = parquetFiles()
      assert(filesBefore.length > 0)

      val sql =
        s"""CALL system.rewrite_data_files(table => \"$tableName\",
           |strategy => 'sort',
           |sort_order => 'zorder(id, partitionKey)',
           |options => map('min-input-files','${filesBefore.length}', 'target-file-size-bytes','49152')
           |)""".stripMargin
      When(s"we execute the SQL:\n${Console.GREEN + sql + Console.RESET}")
      spark.sqlContext.sql(sql)

      val filesAfter = parquetFiles()
      val newFiles = filesAfter.toSet -- filesBefore.toSet
      Then(s"added to the original ${filesBefore.length} files are:\n${alternativeColours(newFiles).mkString("\n")}")
      assert(newFiles.size > 0)

      And("the data in the new files is in order")
      val coords = for {
        file <- newFiles
      } yield {
        val df = spark.read.format("parquet").load(file).as[Datum].collect()
        (df.map(_.id).min, df.map(_.id).max, df.map(_.partitionKey).min, df.map(_.partitionKey).max)
      }
      print(coords.mkString("\n"))
      val areas = coords.map { case (x_min: Int, x_max: Int, y_min: Long, y_max: Long) =>
        (x_max - x_min) * (y_max - y_min)
      }
      val totalArea = (shuffled.map(_.id).max - shuffled.map(_.id).min) * (shuffled.map(_.partitionKey).max - shuffled.map(_.partitionKey).min)
      Then(s"the overlap is ${areas.sum} out of ${totalArea}")
    }
  }

}
