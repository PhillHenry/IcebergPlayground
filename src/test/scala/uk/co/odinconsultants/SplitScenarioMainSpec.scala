package uk.co.odinconsultants
import org.scalatest.wordspec.AnyWordSpec

class plitScenarioMainSpec extends AnyWordSpec {

  import SplitScenariosMain._

  val specLine = "IcebergCRUDSpec:"

  s"The line $specLine" should {
    s"match $DEFAULT_SPEC_DELIMITER_REGEX" in {
      assert(lineMatch(DEFAULT_SPEC_DELIMITER_REGEX, specLine) == Some(specLine.replace(":", "")))
    }
  }

}
