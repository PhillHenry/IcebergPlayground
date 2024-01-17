package uk.co.odinconsultants.iceberg
import org.apache.iceberg.{ContentFile, Snapshot, Table}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object MetaUtils {

  def toFile(x: ContentFile[_]): String = x.path().toString

  def pathsOf(xs: java.lang.Iterable[_ <: ContentFile[_]]): Set[String] =
    xs.asScala.toSet.map(toFile)

  def allFilesThatmake(table: Table): Set[String] = {
    val snapshots                                                       = timeOrderedSnapshots(table)
    val io                                                              = table.io()
    @tailrec
    def files(acc: Set[String], snapshots: List[Snapshot]): Set[String] =
      if (snapshots.isEmpty) acc
      else {
        val snapshot                    = snapshots.head
        val removedDeleted: Set[String] = pathsOf(snapshot.removedDataFiles(io))
        val addedData: Set[String]      = pathsOf(snapshot.addedDataFiles(io))
        files((acc -- removedDeleted) ++ addedData, snapshots.tail)
      }
    files(Set.empty[String], snapshots)
  }

  def timeOrderedSnapshots(table: Table) =
    table.snapshots().asScala.toList.sortBy(_.timestampMillis())
}
