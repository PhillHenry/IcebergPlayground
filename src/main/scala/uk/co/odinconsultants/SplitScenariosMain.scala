package uk.co.odinconsultants
import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process.Process

object SplitScenariosMain {

  val DEFAULT_SPEC_DELIMITER_REGEX = "^(.*)Spec:"
  val HUGO_CONTENT                 = "hugo/content"

  def parse(pattern: String, filename: String): List[String] = {
    println(s"Finding $pattern in $filename")
    val newFiles   = ArrayBuffer[String]()
    val buffer     = Source.fromFile(filename)
    val output     = new ArrayBuffer[String]()
    val dir        = filename.substring(0, filename.lastIndexOf("/"))
    var outputFile = ""
    for (line <- buffer.getLines()) {
      lineMatch(pattern, line).map { file =>
        println(s"$line, $file")
        if (outputFile.length > 0) writeScenario(s"$dir/$outputFile.txt", output)
        outputFile = file
        newFiles.append(s"$dir/$outputFile.txt")
      }
      output.append(line)
    }
    writeScenario(s"$dir/$outputFile.txt", output)
    newFiles.toList
  }

  def lineMatch(regex: String, line: String): Option[String] = {
    val re = regex.r
    line match {
      case re(x) => Some(x)
      case _     => None
    }
  }

  private def writeScenario(
      filename: String,
      output: ArrayBuffer[String],
  ): Unit =
    if (output.nonEmpty) {
      println(s"Writing to $filename")
      Files.write(Paths.get(filename), output.mkString("\n").getBytes())
      output.clear()
    }

  def main(args: Array[String]): Unit = {
    val pattern  = if (args.length == 0) DEFAULT_SPEC_DELIMITER_REGEX else args(0)
    val filename = if (args.length == 0) "target/surefire-reports/scenarios.txt" else args(1)
    val files    = parse(pattern, filename)
    val htmlFiles =  files.map { file =>
      println(s"ansi2html $file")
      val localFile: String = file.substring(file.lastIndexOf("/"))
      val htmlFile          = s"${localFile.substring(0, localFile.lastIndexOf("."))}.html"
      ((Process("ansi2html", new File(HUGO_CONTENT))
        #< new File(file))
        #> new File(s"$HUGO_CONTENT$htmlFile")).!!
      htmlFile
    }
    val header = """## Iceberg Playground
                   |
                   |These are BDD (Behaviour Driven Design) tests that both test
                   |the code and generate human readable documentation.
                   |""".stripMargin
    val md = new ArrayBuffer[String]()
    md.append(header)
    for (file <- htmlFiles.map(_.substring(1)).sorted) {
      md.append(s"[${file.substring(0, file.lastIndexOf("."))}]($file)\n")
    }
    Files.write(Paths.get(s"$HUGO_CONTENT/index.md"), md.mkString("\n").getBytes())
  }

}
