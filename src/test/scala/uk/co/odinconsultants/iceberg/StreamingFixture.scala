package uk.co.odinconsultants.iceberg

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import uk.co.odinconsultants.documentation_utils.Datum

trait StreamingFixture { self: SimpleSparkFixture =>
  def processAllRecordsIn(
                           streamingQuery: StreamingQuery
                         ): Unit = {
    streamingQuery.processAllAvailable()
    streamingQuery.exception.foreach { x =>
      x.printStackTrace()
      throw x
    }
  }

  def startStreamingQuery(df: Dataset[Datum], tableName: String): StreamingQuery = df.writeStream.format("iceberg")
    .outputMode(OutputMode.Append())
    .option("path",               tableName)
    .option("checkpointLocation", s"${dataDir(tableName)}.checkpoint")
    .option("fanout-enabled", "true")
    .partitionBy(partitionField)
    .start()
}
