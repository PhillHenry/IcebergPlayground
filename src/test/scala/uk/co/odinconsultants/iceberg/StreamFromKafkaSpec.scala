package uk.co.odinconsultants.iceberg

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.KafkaForTesting._
import uk.co.odinconsultants.SerializationUtils._
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

import scala.jdk.CollectionConverters._

class StreamFromKafkaSpec
  extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/latest/reliability/")

  import spark.implicits._

  val TopicName = tableName + System.currentTimeMillis()

  "Reading messages from Kafka" should {
    "be written to Iceberg" in new SimpleSparkFixture {
      private val sql: String = tableDDL(tableName, partitionField)
      Given(s"an Iceberg created with:\n${formatSQL(sql)}")
      spark.sql(sql)
      And(s"a Kafka topic called $TopicName")
      val topics = Seq[NewTopic](new NewTopic(TopicName, 1, 1.toShort))
      adminClient.createTopics(topics.asJava)

      When(s"we send ${data.length} records to Kafka")
      for (datum <- data) {
        val record = new ProducerRecord[Int, String](TopicName, datum.id, datum.asJson.toString)
        producer.send(record)
      }

      And(s"read from the Kafka topic writing to the Iceberg table")
      val df = readKafkaViaSpark(TopicName)
      val streamingQuery      = df.writeStream.format("iceberg")
        .outputMode(OutputMode.Append())
        .option("path",               tableName)
        .option("checkpointLocation", s"${dataDir(tableName)}.checkpoint")
        .partitionBy(partitionField)
        .start()

      Then("the table is populated with data")
      processAllRecordsIn(streamingQuery)
      val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(table.count() > 0)
    }
  }

  private def processAllRecordsIn(
      streamingQuery: StreamingQuery
  ): Unit = {
    streamingQuery.processAllAvailable()
    streamingQuery.exception.foreach { x =>
      x.printStackTrace()
      fail(x)
    }
  }

  def tableDDL(tableName: String, partitionField: String): String =
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
       |    'format-version' = '2',
       |    'sort-order' = '$partitionField ASC NULLS FIRST',
       |    'write.distribution-mode' = 'none'
       |) PARTITIONED BY ($partitionField); """.stripMargin

}
