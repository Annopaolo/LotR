//TODO: put names here or in a configuration file
import HelloWorld.names

object Utils {

  def sentToInt : String => Int =
  {
    case "positive" => 1
    case "negative" => -1
    case _ => 0
  }

  def wordList : String => Array[String] =
  {
    _.replaceAll("[,.!?:;]", "")
      .split(" ")
      .filter(names.contains(_))
  }

  def calcIdf(docCount : Int, df : Double) =
    Math.log((docCount + 1)/df + 1)

}
