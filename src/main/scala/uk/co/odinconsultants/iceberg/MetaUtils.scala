package uk.co.odinconsultants.iceberg
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.{ContentFile, Snapshot, Table}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object MetaUtils {

  def toFile(x: ContentFile[_]): String = x.path().toString

  def pathsOf(xs: java.lang.Iterable[_ <: ContentFile[_]]): Set[String] =
    xs.asScala.toSet.map(toFile)

  def allFilesThatmake(table: Table, io: FileIO): Set[String] = {
    val snapshots                                                       = table.snapshots().asScala.toList.sortBy(_.timestampMillis())
    @tailrec
    def files(acc: Set[String], snapshots: List[Snapshot]): Set[String] =
      if (snapshots.isEmpty) acc
      else {
        val snapshot                    = snapshots.head
        val removedDeleted: Set[String] = pathsOf(snapshot.removedDeleteFiles(io))
        val addedDeleted: Set[String]   = pathsOf(snapshot.addedDeleteFiles(io))
        val addedData: Set[String]      = pathsOf(snapshot.addedDataFiles(io))
        files(((acc -- removedDeleted) -- addedDeleted) ++ addedData, snapshots.tail)
      }
    files(Set.empty[String], snapshots)
  }

}
