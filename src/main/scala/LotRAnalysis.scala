import org.apache.spark.sql.SparkSession

object LotRAnalysis {
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
  val spark: SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    args(0).toLowerCase match{
      case "rdd" => RDDAnalyzer.run(names, spark)
      case "df" => DFAnalyzer.run(names, spark)
      case _ => println("Command-line arguments parse error")
    }
  }
}
