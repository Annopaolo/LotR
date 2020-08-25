//TODO: put names here or in a configuration file
import HelloWorld.names
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

object Utils {

  def sentToInt : String => Int =
  {
    case "positive" => 1
    case "negative" => -1
    case _ => 0
  }

  def wordList : String => Array[String] =
    _.replaceAll("[,.!?:;]", "")
      .split(" ")
      .filter(names.contains(_))

  def calcIdf(docCount : Int, df : Double) =
    Math.log((docCount + 1)/df + 1)

  def getSentimentPipeline =
    PretrainedPipeline.fromDisk("src/main/resources/analyze_sentiment_en_2.4.0_2.4_1580483464667");
}
