import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{arrays_zip, explode}

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.Map

object RDDAnalysis {

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

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                .appName("test")
                                .master("local[*]")
                                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val book = spark.read.option("multiline", "true").json("src/main/resources/LordOfTheRingsBook.json").cache();
    val phrases = book.select("ChapterData");
    val pipeline2 = Utils.getSentimentPipeline

    val trans = pipeline2.annotate(phrases, "ChapterData");

    val arrs = trans.select(
      $"sentiment.result".as("sentiment"),
      $"sentence.result".as("sentence")
    );

    val phr = arrs.
      withColumn("r", explode(arrays_zip($"sentiment", $"sentence")))
      .select(
        $"r.sentiment" as("sentiment"),
        $"r.sentence" as("sentence"))

    val myFavouriteRdd = phr.rdd.map(r => (r.getString(0), r.getString(1)));

    val namesBroadcast = spark.sparkContext.broadcast(names);

    val parRdd2 = myFavouriteRdd.zipWithUniqueId()
                    .flatMap{case ((sentiment, sentence),id) =>
                      sentence.replaceAll("[,.!?:;]", "")
                               .split(" ")
                                .map(e => ((Utils.sentToInt(sentiment),e), id))
                    }.filter{case ((_, word),_) => namesBroadcast.value contains word}

    val rddForDf = parRdd2.map(x => (x._2, x._1)).cache()

    val rddForTf = rddForDf.groupByKey()
                           .map{case (id,other) =>
                             (id, (other.head._1, other.map(
                               e => e._2
                             ) toList))
                           }

    val docs = rddForTf.count().toInt
    val docCount = spark.sparkContext.broadcast(docs)

    println(s"There are ${docCount.value} documents to be considered.")
    println("----------SOME DF---------")

    val realRddForDf = rddForDf.map(x => (x._1, x._2._2));
    val df = realRddForDf.map(x => (x._2, x._1)).distinct().groupByKey().map(x => (x._1, x._2.size))//.sortBy(x => x._2);
    df.take(10).foreach(x => println(s"${x._1}, ${x._2}"))

    //TODO: see if it's better compute idf here
    //TODO: try if it's better to keep indexes and join with sentiment
    val tf = rddForTf.map{case (i,(sent, names)) => (i, (sent, listToMap(names) toSeq))}
    //tf.take(10).foreach(x => println(s"${x._1}, sent: ${x._2._1}, ${x._2._2.mkString(",")}"))
    val justTf = tf.map{x => x._2}.flatMap{case (sent,vals) => vals.map(e => (sent, e))}
    //justTf.take(10).foreach(x => println(s"sent: ${x._1}, occurrences: ${x._2}"))

    def computeResult (in : RDD[(String, Int)], computationName : String) : Unit = {
      println(s"-----------$computationName--------------")
      val tfAndDf = in.join(df) //May it be that tf and df are switched?
      val tfIdf = tfAndDf.map{case (name, (tf,df)) => (name, tf * Utils.calcIdf(docCount.value, df))}
      val sumUp = tfIdf.reduceByKey(_+_).sortBy(_._2)
      sumUp.foreach(x => println(s"${x._1}, ${x._2}"))
    }

    val freqs = justTf.map{case (_, (name, freq)) => (name, freq)}
    computeResult(freqs, "simple tf-idf")

    val tfPlusSent = justTf.map{case (sent, (name, freq)) => (name, sent * freq)}
    computeResult(tfPlusSent, "tf-idf + sentiment")
  }

  def listToMap (a:Seq[String]) : IMap[String, Int] = {
    listToMapHelper(a, Map.empty[String,Int]).toMap
  }

  def listToMapHelper (a:Seq[String], acc: Map[String, Int]) : Map[String, Int] = {
    a.foldRight(acc) ((current:String, acc:Map[String, Int]) => {
      if (acc.contains(current)) {
        acc.update(current, acc(current) + 1)
        acc
      }
      else
        acc + (current -> 1)
    })
  }

}