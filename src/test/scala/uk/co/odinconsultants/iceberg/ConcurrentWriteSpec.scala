package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.SpecFormats.prettyPrintSampleOf

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit.MINUTES
import scala.util.{Try, Failure}

class ConcurrentWriteSpec extends AnyWordSpec with GivenWhenThen {
  "Concurrent writes" should {
    val tableName = "spark_file_test_writeTo"

    "cause one transaction to fail" in new SimpleFixture {
      def writeData(): Future[Unit] = Future {
        spark.createDataFrame(data).writeTo(tableName).create()
      }
      Given(s"two threads trying to write data\n${prettyPrintSampleOf(data)}")
      When("both run at the same time")
      val first                     = writeData()
      val second                    = writeData()
      val results                   = for {
        future <- List(first, second)
      } yield Await.ready(future, Duration(1, MINUTES))
      val failures: List[Try[Unit]]  = results.flatMap(_.value.filter(_.isFailure).toList)
      val failure = failures.map(_ match {
        case Failure(exception) => exception
        case _                  => throw new Exception("Was expecting one TX to fail")
      }).head
      Then(s"one fails with exception ${failure}")
      assert(failures.size == 1)
    }
  }
}
