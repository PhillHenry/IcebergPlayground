package uk.co.odinconsultants.iceberg

import org.apache.iceberg.{Files, ManifestFiles, Table}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkForTesting.{spark, _}
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier}

import scala.collection.mutable.{Set => MSet}

class IcebergCRUDSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {
  "A dataset we CRUD" should {
    import spark.implicits._
    val files: MSet[String] = MSet.empty[String]

    "create the appropriate Iceberg files" in new SimpleSparkFixture {
      Given(s"data\n${prettyPrintSampleOf(data)}")
      When(s"writing to table '$tableName'")
      spark.sql(s"DROP TABLE  IF EXISTS $tableName  PURGE")
      spark.createDataFrame(data).writeTo(tableName).create()
      Then("reading the table back yields the same data")
      assertDataIn(tableName)
      files.addAll(parquetFiles(tableName))
      And("there is no mention of the table in the metastore")
      assert(!spark.sessionState.catalog.tableExists(TableIdentifier(tableName)))
      assertThrows[NoSuchTableException] {
        spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
      }
    }

    val newVal    = "ipse locum"
    val updateSql = s"update $tableName set label='$newVal'"
    s"support updates with '$updateSql'" in new SimpleSparkFixture {
      Given(s"SQL ${formatSQL(updateSql)}")
      When("we execute it")
      spark.sqlContext.sql(updateSql)
      Then("all rows are updated")
      val output: Dataset[Datum]  = spark.read.table(tableName).as[Datum]
      val rows: Array[Datum]      = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      assert(rows.length == data.length)
      for {
        row <- rows
      } yield assert(row.label == newVal)
      val dataFiles: List[String] = parquetFiles(tableName).toList
      assert(dataFiles.length > files.size)
      files.addAll(dataFiles)
    }

    val newColumn  = "new_string"
    val alterTable =
      s"ALTER TABLE $tableName ADD COLUMNS ($newColumn string comment '$newColumn docs')"
    s"be able to have its schema updated" in {
      Given(s"SQL ${formatSQL(alterTable)}")
      When("we execute it")
      spark.sqlContext.sql(alterTable)
      Then("all rows are updated")
      val output: DataFrame = spark.read.table(tableName)
      val rows: Array[Row]  = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      for {
        row <- rows
      } yield assert(row.getAs[String](newColumn) == null)
    }

    "be able to have rows removed" in new SimpleSparkFixture {
      private val minID: Int = num_rows / 2
      val deleteSql = s"DELETE FROM $tableName where id < $minID"
      Given(s"SQL ${formatSQL(deleteSql)}")
      val before: Set[String] = dataFilesIn(tableName).toSet
      And(s"the parquet files:\n${toHumanReadable(before)}")
      When("we execute it")
      spark.sqlContext.sql(deleteSql)
      Then(s"there are no longer an rows with ID < $minID")
      assert(spark.sqlContext.sql(s"SELECT COUNT(*) FROM $tableName where id < $minID").collect()(0).getLong(0) == 0)
      val after: Set[String] = dataFilesIn(tableName).toSet
      val deltaFiles: Set[String] = after -- before
      And(s"no files are deleted but there are the following new parquet files:\n${toHumanReadable(deltaFiles)}")
      assert((before -- after).size == 0)
      assert((after -- before).size > 0)
//      val newDFs: DataFrame = spark.read.parquet(deltaFiles.toArray: _*)
//      And(s"those new files contain just the data with id >= $minID")
//      assert(newDFs.select(col("id") >= minID).count() == newDFs.count())
    }

    "can have its history queried" in new SimpleSparkFixture {
      val historySql = s"select * from ${tableName}" + ".history"
      Given("a table that has seen changes")
      When(s"we execute:${formatSQL(historySql)}")

      val history: DataFrame = spark.sqlContext.sql(historySql)
      Then(s"we see entries in the history table thus:\n${captureOutputOf(history.show(truncate = false))}")
      val rows               = history.as[History].collect().sortBy(_.made_current_at)
      assert(rows.length > 0)

      val firstSnapshotID: Long = rows(0).snapshot_id
      val snapshotSql = s"select * from ${tableName} VERSION AS OF $firstSnapshotID"
      And(
        s"""we can view a snapshot image with SQL:${formatSQL(snapshotSql)}
        |${captureOutputOf(spark.sqlContext.sql(snapshotSql).show(truncate = false))}""".stripMargin
      )
    }

    "when vacuumed, have old files removed" ignore new SimpleSparkFixture {
      val table: Table = icebergTable(tableName)
      val filesBefore = dataFilesIn(tableName).toSet
      Given(s"the ${filesBefore.size} files are:\n${toHumanReadable(filesBefore)}")
      table
        .expireSnapshots()
        .expireOlderThan(System.currentTimeMillis())
        .commit()
      When("we call expireSnapshot")
      table.expireSnapshots()
      SparkActions
        .get()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("label", newVal))
        .option("target-file-size-bytes", (500 * 1024 * 1024L).toString) // 500 MB
        .execute()
      val afterBefore = dataFilesIn(tableName).toSet
      Then(s"the original ${filesBefore.size} files now look like:\n${toHumanReadable(afterBefore)}")
      And(s"there are ${(filesBefore -- afterBefore).size} new files")
      val filesNow: List[String] = parquetFiles(tableName).toList
      withClue(filesNow.mkString("\n")){
        assert(filesNow.length < filesBefore.size)
      }
    }

    "should delete all files when dropped" ignore  new SimpleSparkFixture {
      val sqlDrop = s"DROP TABLE $tableName PURGE"
      private val nFiles: Int = parquetFiles(tableName).length
      Given(s"$tableName has $nFiles")
      assert(nFiles > 0) // sanity test
      When(s"we execute:${formatSQL(sqlDrop)}")
      spark.sql(sqlDrop)
      Then("there are no files")
      assert(parquetFiles(tableName).length == 1) // 1 is the directory name
    }
  }
}

case class History(made_current_at: java.sql.Timestamp, snapshot_id: Long, parent_id: Option[Long], is_current_ancestor: Boolean)
