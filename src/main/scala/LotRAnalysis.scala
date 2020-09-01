import org.apache.spark.sql.SparkSession
import Utils.names

object LotRAnalysis {

  val spark: SparkSession = SparkSession.builder()
    .appName("LotRAnalysis")
    .master("local[*]")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    Utils.setResourceLocation(args(1), args(2))
    val res = args(0).toLowerCase match{
      case "rdd" => RDDAnalyzer.run(names, spark)
      case "df" => DFAnalyzer.run(names, spark)
      case _ => throw new IllegalArgumentException("Command-line arguments parse error")
    }
    spark.stop()
    prettyPrint(res._1, "tfidf");
    prettyPrint(res._2, "sentiment + tfidf");
  }

  def prettyPrint(in : Array[(String, Double)], name : String) : Unit = {
    println(s"-----------$name---------------")
    in.foreach(x => println(f"${x._1}%s : ${x._2}%.4f"))
  }
}
