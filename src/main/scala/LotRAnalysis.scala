import org.apache.spark.sql.SparkSession
import Utils.names

object LotRAnalysis {

  val spark: SparkSession = SparkSession.builder()
    .appName("LotRAnalysis")
    //.master("local[*]")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    args(0).toLowerCase match{
      case "rdd" => RDDAnalyzer.run(names, spark)
      case "df" => DFAnalyzer.run(names, spark)
      case _ => println("Command-line arguments parse error")
    }
    spark.stop()
  }
}
