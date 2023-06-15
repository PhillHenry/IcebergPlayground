package uk.co.odinconsultants.iceberg

case class Datum(id: Int, label: String)

trait Fixture[T] {
  def data: Seq[T]
}

trait SimpleFixture extends Fixture[Datum] {

  val data: Seq[Datum] = Seq(Datum(41, "phill"), Datum(42, "henry"))

}
