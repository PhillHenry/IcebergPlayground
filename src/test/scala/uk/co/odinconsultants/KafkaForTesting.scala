package uk.co.odinconsultants
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.Dataset
import uk.co.odinconsultants.SerializationUtils.decodeFromJson
import uk.co.odinconsultants.SparkForTesting.spark
import uk.co.odinconsultants.documentation_utils.Datum

import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaForTesting {
  val KafkaPort = 9092
  val KafkaHost = "127.0.0.1"
  val Endpoint = s"$KafkaHost:$KafkaPort"

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

  def readKafkaViaSpark(topicName: String): Dataset[Datum] = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$KafkaHost:$KafkaPort")
      .option("subscribe", topicName)
      .option("offset", "earliest")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(decodeFromJson)
      .as[Datum]
  }
}
