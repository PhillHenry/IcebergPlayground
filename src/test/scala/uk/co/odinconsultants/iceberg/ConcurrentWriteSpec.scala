package uk.co.odinconsultants.iceberg
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

import java.util.concurrent.TimeUnit.MINUTES
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class ConcurrentWriteSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
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
      val failure = failures.map(_ match {
        case Failure(exception) => exception
        case _                  => throw new Exception("Was expecting one TX to fail")
      }).head
      Then(s"one fails with exception ${failure}")
      assert(failures.size == 1)
      And("one succeeds")
      assertDataIn(tableName)
    }
  }
}
