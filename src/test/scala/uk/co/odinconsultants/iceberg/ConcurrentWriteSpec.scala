package uk.co.odinconsultants.iceberg
import org.apache.spark.sql.Dataset
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import java.util.concurrent.TimeUnit.MINUTES
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class ConcurrentWriteSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/latest/reliability/")

  import spark.implicits._

  "Concurrent writes" should {
    "cause one transaction to fail" in new SimpleSparkFixture {
      def writeData(): Future[Unit] = Future {
        spark.createDataFrame(data).writeTo(tableName).create()
      }
      Given(s"two transactions trying to write data\n${prettyPrintSampleOf(data)}")
      When("both run at the same time")
      val first                     = writeData()
      val second                    = writeData()
      val results                   = for {
        future <- List(first, second)
      } yield Await.ready(future, Duration(1, MINUTES))
      val failures: List[Try[Unit]]  = results.flatMap(_.value.filter(_.isFailure).toList)
      assert(failures.size == 1)
      val failure = failures.map(_ match {
        case Failure(exception) =>
          exception.printStackTrace()
          exception
        case _                  => throw new Exception("Was expecting one TX to fail")
      }).head
      Then(s"one fails with exception ${failure}")
      assert(failures.size == 1)
      And("one succeeds")
      assertDataIn(tableName)
    }
  }

  "Data integrity" should {
    "be intact after a failure" in new SimpleSparkFixture {
      Given(s"the table '$tableName' has had a failed write")
      When(s"we call spark.read.table(\"$tableName\")")
      val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      private val nRows: Long = table.count()
      Then(s"the table still contains $nRows records")
      assert(nRows == data.length)
      And("failed files are left behind")
      private val dir: String = dataDir(tableName)
      print(s"About to read: $dir")
      val raw: Dataset[Datum] = spark.read.parquet(dir).as[Datum]
      assert(raw.count() > nRows)
    }
  }

}
