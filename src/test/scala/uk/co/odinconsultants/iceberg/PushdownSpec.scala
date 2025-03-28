package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.functions._
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.TextUtils.emphasise
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

class PushdownSpec extends SpecPretifier with GivenWhenThen with TableNameFixture with UpdatingTable {

  import spark.implicits._

  s"Predicates" should {
    s"not be pushed down" in new SimpleSparkFixture {
      spark.createDataFrame(data).writeTo(tableName).create()
      Given(s"a table partitioned by $partitionField")
      val queryPlan = captureOutputOf {
        spark.read.table(tableName).as[Datum].where(concat(lit("2025-01-0"), col(partitionField)) === lit("2025-01-02")).explain(extended = false)
      }
      When(s"we execute a query that concatenates strings based on $partitionField")
      val noFilters = "filters=,"
      Then(s"The query plan:\n${emphasise(noFilters, queryPlan)}\nshows nothing has been pushed down")
      assert(queryPlan.contains(noFilters)) // ie, there are no filters to be pushed down
    }

    s"be pushed down" in new SimpleSparkFixture {
      Given(s"the same table containing data that looks like:\n${prettyPrintSampleOf(data)}")
      When(s"we execute a query that simply checks the equality of $partitionField")
      val queryPlan = captureOutputOf {
        spark.read.table(tableName).as[Datum].where(col(partitionField) === 1).explain(extended = false)
      }
      val filtered = s"filters=$partitionField"
      Then(s"the query plan:\n${emphasise(filtered, queryPlan)}")
      assert(queryPlan.contains(filtered))
    }
  }

}
