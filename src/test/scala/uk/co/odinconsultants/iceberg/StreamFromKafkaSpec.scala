package uk.co.odinconsultants.iceberg

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import java.util.Properties
import scala.jdk.CollectionConverters._
import io.circe.syntax._
import io.circe.generic.auto._

import java.text.SimpleDateFormat
import io.circe.{Decoder, Encoder, Json}
import org.apache.spark.sql.streaming.OutputMode

import java.sql.{Date, Timestamp}
import scala.util.Try


class StreamFromKafkaSpec
  extends SpecPretifier with GivenWhenThen with TableNameFixture {

  // Custom date format for serialization
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  // Encoder and Decoder for java.sql.Date
  implicit val dateEncoder: Encoder[Date] = Encoder.instance(date => Json.fromString(dateFormat.format(date)))
  implicit val dateDecoder: Decoder[Date] = Decoder.decodeString.emap { str =>
    Try ( new Date(dateFormat.parse(str).getTime) ).toEither.left.map(_ => "Date")
  }

  // Encoder and Decoder for java.sql.Timestamp
  implicit val timestampEncoder: Encoder[Timestamp] = Encoder.instance(timestamp => Json.fromString(timestampFormat.format(timestamp)))
  implicit val timestampDecoder: Decoder[Timestamp] = Decoder.decodeString.emap { str =>
    Try(new Timestamp(timestampFormat.parse(str).getTime)).toEither.left.map(_ => "Timestamp")
  }

  info("https://iceberg.apache.org/docs/latest/reliability/")

  import spark.implicits._

  val KafkaPort = 9092
  val KafkaHost = "127.0.0.1"
  val Endpoint = s"$KafkaHost:$KafkaPort"
  val TopicName = tableName + System.currentTimeMillis()

  "Reading from Kafka" should {
    "be written to Iceberg" in new SimpleSparkFixture {
      val adminClient = AdminClient.create(
        Map[String, Object](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG       -> Endpoint,
          AdminClientConfig.CLIENT_ID_CONFIG               -> "test-kafka-admin-client",
          AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG      -> "10000",
          AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> "10000",
        ).asJava
      )
      val props = new Properties()
      props.put("bootstrap.servers",                Endpoint)
      props.put("port",                             KafkaPort)
      props.put("broker.id",                        "0")
      props.put("num.partitions",                   "1")
      props.put("key.serializer",                   classOf[org.apache.kafka.common.serialization.IntegerSerializer].getName)
      props.put("value.serializer",                 classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
      props.put("offsets.topic.replication.factor", "1")
      props.put("auto.offset.reset",                "earliest")
      props.put("host.name",                        KafkaHost)
      props.put("advertised.host.name",             KafkaHost)
      val producer = new KafkaProducer[Int, String](props)
      val topics = Seq[NewTopic](new NewTopic(TopicName, 1, 1.toShort))
      adminClient.createTopics(topics.asJava)
      adminClient.close()

      for (datum <- data) {
        val record = new ProducerRecord[Int, String](TopicName, datum.id, datum.asJson.toString)
        producer.send(record)
      }

//      val df = spark
//        .readStream
//        .format("kafka")
//        .option("kafka.bootstrap.servers",  s"$KafkaHost:$KafkaPort")
//        .option("subscribe",                TopicName)
//        .option("offset",                   "earliest")
//        .option("startingOffsets",          "earliest")
//        .load()
//
//      val dir = dataDir(tableName)
//      val streamingQuery      = df.writeStream.format("iceberg")
//        .outputMode(OutputMode.Append())
//        .option("path",               dir)
//        .option("checkpointLocation", s"${dir}.checkpoint")
//        .partitionBy("partitionKey")
//        .start()

    }
  }

}