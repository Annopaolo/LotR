object Utils {

  val bookPath = "/home/annopaolo/Scrivania/scala/LotR/src/main/resources/LordOfTheRingsBook.json"
  val pipelinePath = "/home/annopaolo/Scrivania/scala/LotR/src/main/resources/analyze_sentiment_en_2.4.0_2.4_1580483464667"

  val names = Seq(
    "Galadriel",
    "Bilbo",
    "Frodo",
    "Sam",
    "Gandalf",
    "Aragorn",
    "Legolas",
    "Gimli",
    "Gollum",
    "Bombadil",
    "Smeagol",
    "Sauron",
    "Saruman",
    "Merry",
    "Pippin",
    "Boromir",
    "Faramir",
    "Treebeard",
    "Elrond"
  )

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
}
