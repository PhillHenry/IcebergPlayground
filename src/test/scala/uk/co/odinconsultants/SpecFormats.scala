package uk.co.odinconsultants
import com.github.vertical_blank.sqlformatter.SqlFormatter.format

trait SpecFormats {

  def classNameOf(all: Seq[_]): String = all.head.getClass.getSimpleName

  def inTheRangeOf(range: Range): String = s"in the range from ${range.start} to ${range.`end`}"

  def prettyPrintSampleOf[T](xs: Iterable[T]): String = {
    val sampleSize = 3
    val sample: List[String] = alternativeColours(xs.take(sampleSize))
    val footer: String = if (xs.size > sampleSize) "..." else ""
    s"${indent(sample).mkString("\n")}$footer"
  }

  def indent[A](xs: Iterable[A]): Iterable[String] = xs.map((x: A) => s"\t\t\t$x")

  val scenarioDelimiter = s"\n${"+ " * 20}\n+\n"

  def formatSQL(sql: String): String = s"\n${Console.YELLOW}${format(sql)}${Console.RESET}"

  def toHumanReadable[T](rows: Iterable[T]): String = alternativeColours(rows).mkString("\n")

  def alternativeColours[T](xs: Iterable[T]): List[String] = {
    xs.zipWithIndex.map { case (x: T, index: Int) =>
      val colour = if (index % 2 == 0) Console.YELLOW else Console.MAGENTA
      s"$colour$x"
    }.toList :+ s"${Console.RESET}"
  }

  def delimiter(n: Int): String = {
    val colours = List(
      Console.MAGENTA,
      Console.YELLOW,
      Console.WHITE,
      Console.CYAN,
      Console.BLUE,
      Console.RED,
      Console.GREEN,
    )
    (0 to n)
      .map { n =>
        s"${colours(n % colours.length)}+ "
      }
      .mkString("") + Console.RESET
  }
}

