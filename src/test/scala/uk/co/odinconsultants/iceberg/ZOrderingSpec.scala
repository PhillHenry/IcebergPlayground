package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import scala.util.Random

class ZOrderingSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import spark.implicits._

  info("https://iceberg.apache.org/docs/1.4.2/spark-procedures/")

  val initialMaxId = 20000

  def idRanges(newFiles: List[String]): List[(Int, Int)] = for {
    file <- newFiles
  } yield {
    val df = spark.read.format("parquet").load(file).as[Datum].collect()
    (df.map(_.id).min, df.map(_.id).max)
  }

  def checkNoOverlappingIds(newFiles: List[String], expectedNumIds: Int): String = {
    val rangeIds: List[(Int, Int)] = idRanges(newFiles)
    val ids: Seq[Int]              = explode(rangeIds)
    assert(ids.size == expectedNumIds)
    s"there is no overlap in the `id` dimension. The ranges of the id look like:\n${alternativeColours(sortedHumanReadable(rangeIds))
        .mkString("\n")}"
  }

  private def sortedHumanReadable(rangeIds: List[(Int, Int)]) =
    rangeIds.sortBy(_._1).map { case (min, max) => s"$min to $max" }

  private def explode(rangeIds: List[(Int, Int)]): Seq[Int] = rangeIds.flatMap {
    case (x_min: Int, x_max: Int) =>
      (x_min to x_max).toList
  }

  s"A table with no particular order" should {
    s"have z-ordered files after rewriting the data" in new SimpleSparkFixture {
      override def num_rows: Int = initialMaxId

      val shuffled    = Random.shuffle(data)
      Given(
        s"$num_rows rows of data that look like\n${prettyPrintSampleOf(shuffled)}\nare initially written to table '$tableName'"
      )
      spark.createDataFrame(shuffled).writeTo(tableName).create()
      val filesBefore = parquetFiles(tableName)
      assert(filesBefore.length > 0)

      val sql =
        s"""CALL system.rewrite_data_files(table => \"$tableName\",
           |strategy => 'sort',
           |sort_order => 'zorder(id, date)',
           |options => map('min-input-files','${filesBefore.length}', 'target-file-size-bytes','49152')
           |)""".stripMargin
      When(s"we execute the SQL:\n${Console.GREEN + sql + Console.RESET}")
      spark.sqlContext.sql(sql)

      val filesAfter = parquetFiles(tableName)
      val newFiles   = (filesAfter.toSet -- filesBefore.toSet).toList
      Then(s"added to the original ${filesBefore.length} files are:\n${alternativeColours(newFiles).mkString("\n")}")
      assert(newFiles.size > 0)

      And(checkNoOverlappingIds(newFiles, num_rows))
    }

    s"needs its data rewritten to maintain its z-order" in new SimpleSparkFixture {
      override def num_rows: Int = initialMaxId

      Given(s"a z-ordered table called '$tableName'")
      val shifted                          = Random.shuffle(data.map(x => x.copy(id = x.id + initialMaxId)))
      When(
        s"$num_rows rows of new data that look like\n${prettyPrintSampleOf(shifted)}\nare appended to table '$tableName'"
      )
      val filesBefore                      = parquetFiles(tableName)
      spark.createDataFrame(shifted).writeTo(tableName).append()
      val newFiles                         = (parquetFiles(tableName).toSet -- filesBefore.toSet).toList
      private val ranges: List[(Int, Int)] = idRanges(newFiles)
      Then(
        s"the ranges of the ids overlap in the ${newFiles.length} new files and look like:\n${alternativeColours(sortedHumanReadable(ranges)).mkString("\n")}"
      )
      assert(explode(ranges).length > num_rows)
    }
  }

}
