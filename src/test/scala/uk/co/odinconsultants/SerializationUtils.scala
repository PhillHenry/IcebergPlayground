package uk.co.odinconsultants
import io.circe.{Decoder, Encoder, Json}
import uk.co.odinconsultants.documentation_utils.Datum

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.util.Try

object SerializationUtils {
  // Custom date format for serialization
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  // Encoder and Decoder for java.sql.Date
  implicit val dateEncoder: Encoder[Date] = Encoder.instance(date => Json.fromString(dateFormat.format(date)))
  implicit val dateDecoder: Decoder[Date] = Decoder.decodeString.emap { str =>
    Try ( new Date(dateFormat.parse(str).getTime) ).toEither.left.map(_ => "Date")
  }

  // Encoder and Decoder for java.sql.Timestamp
  implicit val timestampEncoder: Encoder[Timestamp] = Encoder.instance(timestamp => Json.fromString(timestampFormat.format(timestamp)))
  implicit val timestampDecoder: Decoder[Timestamp] = Decoder.decodeString.emap { str =>
    Try(new Timestamp(timestampFormat.parse(str).getTime)).toEither.left.map(_ => "Timestamp")
  }

  import io.circe._
  import io.circe.generic.auto._
  import io.circe.parser._
  val decodeFromJson: String => Datum = { x: String =>
    val result: Either[Error, Datum] = decode[Datum](x)
    result match {
      case Right(x) => x
      case Left(x)  => ???
    }
  }
}
