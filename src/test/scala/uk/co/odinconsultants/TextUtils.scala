package uk.co.odinconsultants

object TextUtils {

  val Emphasis: String = Console.YELLOW

  def emphasise(selection: String, context: String, continueFormat: String = ""): String =
    context.replace(selection, s"${Console.BOLD}$selection${Console.RESET}$continueFormat")

  def highlight(x: String): String = s"\n$Emphasis$x${Console.RESET}"
}
