package uk.co.odinconsultants.iceberg

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.KafkaForTesting._
import uk.co.odinconsultants.SerializationUtils._
import uk.co.odinconsultants.SparkForTesting
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SimpleFixture, SpecPretifier}
import uk.co.odinconsultants.iceberg.SQL.createDatumTable

import scala.jdk.CollectionConverters._

class StreamFromKafkaSpec
  extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("https://iceberg.apache.org/docs/latest/reliability/")

  import spark.implicits._

  val TopicName = tableName + System.currentTimeMillis()

  "Reading messages from Kafka" should {
    "be written to Iceberg" in new SimpleSparkFixture with StreamingFixture {
      override def num_rows = 1024
      val numPartitions = SparkForTesting.numThreads * 2
      override val data = createData(numPartitions, SimpleFixture.now, dayDelta, tsDelta)
      private val sql: String = tableDDL(tableName, partitionField)
      Given(s"an Iceberg created with:\n${formatSQL(sql)}")
      spark.sql(sql)
      And(s"a Kafka topic called $TopicName")
      val numKafkaPartitions = numPartitions * 3
      val topics = Seq[NewTopic](new NewTopic(TopicName, numKafkaPartitions, 1.toShort))
      adminClient.createTopics(topics.asJava)

      When(s"we send ${data.length} records to Kafka")
      for (datum <- data) {
        val record = new ProducerRecord[Int, String](TopicName, datum.id, datum.asJson.toString)
        producer.send(record)
      }

      And(s"read from the Kafka topic writing to the Iceberg table")
      val df = readKafkaViaSpark(TopicName)//.orderBy(col(TestUtils.idField))
      Then("the table is populated with data")
      processAllRecordsIn(startStreamingQuery(df, tableName))
      val table: Dataset[Datum] = spark.read.table(tableName).as[Datum]
      assert(table.count() == data.length)
      And(s"the number of data files (${parquetFiles(tableName).size}) is the same as the number of Spark " +
        s"partitions, not the number of Kafka partitions ($numKafkaPartitions)")
      assert(parquetFiles(tableName).size == numPartitions)
    }
  }

  def tableDDL(tableName: String, partitionField: String): String =
    s"""${createDatumTable(tableName)} TBLPROPERTIES (
       |    'format-version' = '2',
       |    'sort-order' = '$partitionField ASC NULLS FIRST',
       |    'write.distribution-mode' = 'none'
       |) PARTITIONED BY ($partitionField); """.stripMargin

}
