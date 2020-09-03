import org.apache.spark.sql.SparkSession
import Utils.names

import scala.#::

object LotRAnalysis {

  val spark: SparkSession = SparkSession.builder()
    .appName("LotRAnalysis")
    //.master("local[*]")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    Utils.setResourceLocation(args(1), args(2), args(3))
    val res = args(0).toLowerCase match{
      case "rdd" => RDDAnalyzer.run(names, spark)
      case "df" => DFAnalyzer.run(names, spark)
      case _ => throw new IllegalArgumentException("Command-line arguments parse error")
    }
    spark.sparkContext.stop()
    //toFile (prettify(res._1, "tfidf"), Utils.outputPath)
    //toFile (prettify(res._2, "tfidf*sentiment"), Utils.outputPath)

    print (prettify(res._1, "tfidf"))
    print (prettify(res._2, "tfidf*sentiment"))
  }

  def toFile(from: String, to: String) = {
    import java.io._
    val file = new File(to)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.append(from)
    bw.flush()
    bw.close()
  }

  def prettify(in : Array[(String, Double)], name : String) : String = {
    val l = s"-----------$name---------------" :: (in.sortBy(_._2).map(x => f"${x._1}%s : ${x._2}%.4f")).toList
    l
      .zip(
        1 to l.size map (_ => "\n")
        )
      .flatMap{case (x,y) => Seq(x,y)}.reduce(_++_)
  }
}
