import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
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
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      //.config("spark.driver.memory", "12G")
      //.config("spark.kryoserializer.buffer.max", "200M")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val book = spark.read.option("multiline", "true").json("src/main/resources/LordOfTheRingsBook.json").cache();
    val phrases = book.select("ChapterData");
    val pipeline2 = PretrainedPipeline.fromDisk("src/main/resources/analyze_sentiment_en_2.4.0_2.4_1580483464667");

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
/*
    phr.rdd.take(1).foreach(r => println(r.schema))
    phr.rdd.take(1).foreach(r => println(r.getString(0)))
    phr.rdd.take(1).foreach(r => println(r.getString(1)))*/

    val myFavouriteRdd = phr.rdd.map(r => (r.getString(0), r.getString(1)));
    //myFavouriteRdd.take(10).foreach(println);

    val rdd2 = myFavouriteRdd
      .map{case (sentiment, sentence) => (Utils.sentToInt(sentiment), Utils.wordList(sentence))}
      .filter{case (_, sents) => !(sents isEmpty)}

    def sentAndWordsToString : (Int, Array[String]) => String =
    {case (sentiment, sentence) => (s"(${sentiment}, ${sentence mkString(",")})")}

    //rdd2.take(10).foreach {case (sentiment, sentence) => println(sentAndWordsToString(sentiment,sentence))}

    val rdd3 = rdd2.zipWithUniqueId().map(x => (x._2, x._1)).cache()
    //rdd3.take(10).foreach((x:(Long, (Int, Array[String]))) => println(s"${x._1}, ${sentAndWordsToString(x._2._1, x._2._2)}"))

    val docCount = rdd3.count() toInt;
    println(s"There are $docCount documents.")

    val rddForDf = rdd3.flatMap{case (index, vals) => vals._2.map(e => (index,e)).toList}
    //rddForDf.take(10).foreach(x => println(s"${x._1}, ${x._2}"))

    println("----------SOME DF---------")

    val df = rddForDf.map(x => (x._2, x._1)).distinct().groupByKey().map(x => (x._1, x._2.size))//.sortBy(x => x._2);
    df.take(10).foreach(x => println(s"${x._1}, ${x._2}"))

    //TODO: see if it's better compute idf here
    //val idf = df.map(x => (x._1, ))

    //rdd3.take(10).foreach((x:(Long, (Int, Array[String]))) => println(s"${x._1}, ${sentAndWordsToString(x._2._1, x._2._2)}"))

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
      //r.toMap
    }

    val rddForTf = rdd3;
    //val rddForTf = rdd3.map{case (i, vals) => (i, vals._2)}
    //TODO: try if it's better to keep indexes and join with sentiment
    val tf = rddForTf.map{case (i,(sent, names)) => (i, (sent, listToMap(names) toSeq))}
    //tf.take(10).foreach(x => println(s"${x._1}, sent: ${x._2._1}, ${x._2._2.mkString(",")}"))
    val justTf = tf.map{x => x._2}.flatMap{case (sent,vals) => vals.map(e => (sent, e))}
    //justTf.take(10).foreach(x => println(s"${x._1}, ${x._2}"))


    def justTfIdf : Unit = {
      println("TFIDF")
      val tf = justTf.map{case (_, (name, freq)) => (name, freq)}
      val tfAndDf = tf.join(df) //May it be that tf and df are switched?
      val tfIdf = tfAndDf.map{case (name, (tf,df)) => (name, tf * Utils.calcIdf(docCount, df))}
      val sumUp = tfIdf.reduceByKey(_+_).sortBy(_._2)
      sumUp.foreach(x => println(s"${x._1}, ${x._2}"))
      println("End TFIDF")
    }

    justTfIdf


    val tfPlusSent = justTf.map{case (sent, (name, freq)) => (name, sent * freq)}
    //tfPlusSent.take(10).foreach(x => println(s"${x._1}, ${x._2}"))
    tfPlusSent.filter{case (character, value) =>character == "Frodo"}
      .reduceByKey(_+_)
      .take(1)
      .foreach(x => println("Frodo words * sent: " + x._2))

    //TFIDF
    val tfAndDf = tfPlusSent.join(df)
    //tfAndDf.take(10).foreach(x => println(s"${x._1}, (${x._2._1}, ${x._2._2})"))

    //println("----------SOME TFIDF---------")

    val tfIdf = tfAndDf.map{case (name, (tf,df)) => (name, tf * Utils.calcIdf(docCount, df))}
    //tfIdf.take(10).foreach(x => println(s"${x._1}, (${x._2})"))

    println("----------END---------")
    val sumUp = tfIdf.reduceByKey(_+_).sortBy(_._2)
    sumUp.foreach(x => println(s"${x._1}, ${x._2}"))

  }


}