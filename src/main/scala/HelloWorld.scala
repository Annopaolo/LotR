import java.util.UUID

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.util.{Benchmark, PipelineModels}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HelloWorld {

  val names = Seq("Galadriel", "Bilbo", "Frodo", "Sam", "Gandalf", "Aragorn", "Legolas", "Gimli", "Gollum", "Bombadil", "Smeagol")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config("spark.driver.memory", "12G")
      .config("spark.kryoserializer.buffer.max", "200M")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val book = spark.read.option("multiline", "true").json("src/main/resources/LordOfTheRingsBook.json").cache();
    //book.printSchema();
    val phrases = book.select("ChapterData");
    //phrases.show(10);
    //val pipeline2 = PretrainedPipeline("analyze_sentiment", "en");
    val pipeline2 = PretrainedPipeline.fromDisk("src/main/resources/analyze_sentiment_en_2.4.0_2.4_1580483464667");

    val trans = pipeline2.annotate(phrases, "ChapterData");
    //trans.show(10);
    //trans.printSchema()

    val arrs = trans.select(
      col("sentiment.result").as("sentiment"),
      col("sentence.result").as("sentence")
      //, col("token.result").as("tokens")
    );
    //arrs.show(trans.count() toInt);
    //arrs.printSchema()

    val phr = arrs.
      withColumn("r", explode(arrays_zip($"sentiment", $"sentence")))
      .select(
        $"r.sentiment" as("sentiment"),
        $"r.sentence" as("sentence"))

    //phr.show(10)

    val sentToInt : String => Int =
      {
        case "positive" => 1
        case "negative" => -1
        case _ => 0
      }

    val udfSentToInt = udf(sentToInt)

    val end = phr.withColumn("#sent", udfSentToInt($"sentiment"))
      .select($"#sent" as("sentiment"), $"sentence");
    //end.show(10);

    val _wordList : String => Array[String] = {
      _.replaceAll("[,.!?:;]", "").split(" ").filter( s => names.contains(s))
    }

    val wordList = udf(_wordList);

    val sentPlusList = end.withColumn("words", wordList($"sentence"))
      .select($"words" as("characters"), $"sentiment")
      .filter(size($"characters") > 0);
    //sentPlusList.show(10);

    //START TF-IDF

    //TF
    val documents = sentPlusList.withColumn("id", monotonically_increasing_id());
    documents.show(10);


    val columns = documents.columns.map(col) :+
      (explode(col("characters")) as "token")
    val unfoldedDocs = documents.select(columns: _*).cache();
    //unfoldedDocs.show(10)

    val tokensWithTf =
      unfoldedDocs.groupBy("id", "token")
        .agg(count("characters") as "tf");
    //tokensWithTf.show(10)

    //look for an ID on a column: regex ((\|([\s])+ID\|) [\s]+)

    //IDF

    val tokensWithDf = unfoldedDocs.groupBy("token")
      .agg(countDistinct("id") as "df")

    val dNorm = tokensWithDf.count().toInt
    println(s"${dNorm} documents");

    val calcIdfUdf = udf { df:Int => calcIdf(dNorm, df toDouble) }

    val withIdf = tokensWithDf.withColumn("idf", calcIdfUdf(col("df")))
    //withIdf.show(10)

    //TF+IDF
    val tfidfs = tokensWithTf
      .join(withIdf, Seq("token"), "left")
      .withColumn("tf_idf", $"tf" * $"idf")

    tfidfs.show(10)

    //TFIDF + SENTIMENT

    val temp = tfidfs
      .join(documents, Seq("id"), "left")
      .withColumn("tf_idf_sent", $"tf_idf" * $"sentiment")

    temp.show(10)

    //SUM UP
    temp
      .select($"token" as("character"), $"tf_idf_sent")
      .groupBy("character")
      .agg(sum("tf_idf_sent") as "overall goodness")
      .sort($"overall goodness")
      .show()

  }

  def calcIdf(docCount : Int, df : Double) =
    Math.log((docCount + 1)/df + 1)
}