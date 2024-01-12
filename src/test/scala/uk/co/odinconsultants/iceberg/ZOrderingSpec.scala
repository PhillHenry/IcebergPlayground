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
           |sort_order => 'zorder(id, date)',
           |options => map('min-input-files','${filesBefore.length}', 'target-file-size-bytes','49152')
           |)""".stripMargin
      When(s"we execute the SQL:\n${Console.GREEN + sql + Console.RESET}")
      spark.sqlContext.sql(sql)

      val filesAfter = parquetFiles()
      val newFiles = filesAfter.toSet -- filesBefore.toSet
      Then(s"added to the original ${filesBefore.length} files are:\n${alternativeColours(newFiles).mkString("\n")}")
      assert(newFiles.size > 0)

      val coords = for {
        file <- newFiles
      } yield {
        val df = spark.read.format("parquet").load(file).as[Datum].collect()
        (df.map(_.id).min, df.map(_.id).max, df.map(_.date.getTime).min, df.map(_.date.getTime).max)
      }
      println(coords.toList.sortBy(_._1).mkString("\n"))
      val xs = coords.toList.flatMap { case (x_min: Int, x_max: Int, _: Long, _: Long) =>
        (x_min to x_max).toList
      }
      val totalArea = (shuffled.map(_.id).max - shuffled.map(_.id).min) * (shuffled.map(_.date.getTime).max - shuffled.map(_.date.getTime).min)
      And("there is no overlap in the `id` dimension")
      assert(xs.size == num_rows)
    }
  }

}
