package uk.co.odinconsultants.polaris
import org.apache.polaris.service.PolarisApplication

object PolarisUtils {

  def main(args: Array[String]): Unit = {
    new PolarisApplication().run("server")
  }

}
