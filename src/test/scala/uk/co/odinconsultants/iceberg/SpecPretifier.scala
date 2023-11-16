package uk.co.odinconsultants.iceberg
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import uk.co.odinconsultants.SpecFormats.delimiter

trait SpecPretifier extends AnyWordSpec with BeforeAndAfterAll with BeforeAndAfterEach  {

  override def beforeAll(): Unit = delimit(delimiter(50))

  override def afterEach(): Unit = delimit(delimiter(50))

  def delimit(x: String): Unit = info(x)

}
