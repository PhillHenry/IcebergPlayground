package uk.co.odinconsultants

object SpecFormats {

  def classNameOf(all: Seq[_]): String = all.head.getClass.getSimpleName

  def inTheRangeOf(range: Range): String = s"in the range from ${range.start} to ${range.`end`}"

  def prettyPrintSampleOf[T](xs: Iterable[T]): String = {
    val sampleSize = 3
    s"${indent(xs.take(sampleSize)).mkString("\n")}${if (xs.size > sampleSize) s"\n${indent(Seq("...")).mkString("\n")}" else ""}"
  }

  def indent[A](xs: Iterable[A]): Iterable[String] = xs.map((x: A) => s"\t\t\t$x")

  val scenarioDelimiter = s"\n${"+ " * 40}\n+\n"

}
