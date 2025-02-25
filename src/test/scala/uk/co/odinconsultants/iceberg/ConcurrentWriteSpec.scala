package uk.co.odinconsultants.iceberg
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import java.text.SimpleDateFormat
import java.util.Date
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
      def writeData(): Future[Unit]        = Future {
        val df: Dataset[Datum] = spark.createDataFrame(data).as[Datum]
        df.map(ConcurrentWriteSpec.delayingFn(num_rows / 10)).writeTo(tableName).create()
      }
      def writeDataNoDelay(): Future[Unit] = Future {
        spark.createDataFrame(data).writeTo(tableName).create()
      }
      Given(s"two transactions trying to write data\n${prettyPrintSampleOf(data)}")
      When("both run at the same time")
      val first                            = writeData()
      val second                           = writeDataNoDelay()
      val results                          = for {
        future <- List(first, second)
      } yield Await.ready(future, Duration(1, MINUTES))
      val failures: List[Try[Unit]]        = results.flatMap(_.value.filter(_.isFailure).toList)
      assert(failures.size == 1)
      val failure                          = failures
        .map(_ match {
          case Failure(exception) =>
            exception.printStackTrace()
            exception
          case _                  => throw new Exception("Was expecting one TX to fail")
        })
        .head
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
      private val nRows: Long   = table.count()
      Then(s"the table still contains $nRows records")
      assert(nRows == data.length)
      And("failed files are left behind")
      val raw: Dataset[Datum]   = spark.read.parquet(dataDir(tableName)).as[Datum]
      assert(raw.count() > nRows)
    }
  }

  "Orphans" should {
    "be deleted" in new SimpleSparkFixture {
      val filesBefore = Set(dataFilesIn(tableName))

      val directoryCount: Long = spark.read.parquet(dataDir(tableName)).count()
      assert(directoryCount == data.length * 2)
      Given(s"there are $directoryCount raw rows in the directory when there should be ${data.length}")

      /** java.lang.IllegalArgumentException: Cannot remove orphan files with an interval less
        * than 24 hours. Executing this procedure with a short interval may corrupt the table if
        * other operations are happening at the same time. If you are absolutely confident that no
        * concurrent operations will be affected by removing orphan files with such a short interval,
        * you can use the Action API to remove orphan files with an arbitrary interval.
        *       at org.apache.iceberg.spark.procedures.RemoveOrphanFilesProcedure.validateInterval(RemoveOrphanFilesProcedure.java:209)
        */
//      spark.sqlContext.sql(s"""CALL system.remove_orphan_files(
//                              |    table => '$tableName',
//                              |    older_than => TIMESTAMP '${dateFormat.format(new java.util.Date())}'
//                              |    )""".stripMargin)

      When(s"we use the Java API to call deleteOrphanFiles on anything older than now")
      val actions = SparkActions.get(spark)
      val action  = actions.deleteOrphanFiles(icebergTable(tableName))
      action.olderThan(new Date().getTime)
      action.execute()

      Then(s"old files are deleted and the raw row count is now ${data.length}")
      val filesAfter = Set(dataFilesIn(tableName))
      print(filesBefore -- filesAfter)
      assert(filesBefore != filesAfter)
      assert(spark.read.parquet(dataDir(tableName)).count() == data.length)
    }
  }

}

object ConcurrentWriteSpec {
  def delayingFn(trigger: Int): Datum => Datum = { x =>
    if (x.id == trigger) {
      println("Pausing...")
      Thread.sleep(2000)
    }
    x
  }
}
