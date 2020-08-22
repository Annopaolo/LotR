import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


object HelloWorld {

  val names = Seq("Galadriel", "Bilbo", "Frodo", "Sam", "Gandalf", "Aragorn", "Legolas", "Gimli", "Gollum", "Bombadil", "Smeagol")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      //.config("spark.driver.memory", "12G")
      //.config("spark.kryoserializer.buffer.max", "200M")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val book = spark.read.option("multiline", "true").json("src/main/resources/LordOfTheRingsBook.json").cache();
    //book.printSchema();
    val phrases = book.select("ChapterData");
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

    val phr = arrs.
      withColumn("r", explode(arrays_zip($"sentiment", $"sentence")))
      .select(
        $"r.sentiment" as("sentiment"),
        $"r.sentence" as("sentence"))

    //phr.show(10)
    //TODO: from now on, the code should not use dataframes

    val udfSentToInt = udf(Utils.sentToInt)

    val end = phr.withColumn("#sent", udfSentToInt($"sentiment"))
      .select($"#sent" as("sentiment"), $"sentence");
    //end.show(10);

    val wordList = udf(Utils.wordList);

    val sentPlusList = end.withColumn("words", wordList($"sentence"))
      .select($"words" as("characters"), $"sentiment")
      .filter(size($"characters") > 0);


    val rddAnalysis = {
      //sentPlusList.show();
      val rdd = sentPlusList.rdd;
      val rdd2 = rdd.zipWithUniqueId().cache()
      //rdd2.take(10).foreach(println)


      val toDf = rdd2.map(row => row match {
        case (wrapped, index) => {
          wrapped
        }
      })


      //toDf.take(10).foreach(println);
      //toDf.take(1).foreach(row => println(row.schema))

      //def rowToSensedThing : Row =>

      def explodeCharactersArray: (Seq[String], Int) => List[(String, Int)] = (chars, idx) => {
        chars.map(name => (name, idx)).toList
      }
      //toDf.flatMap(explodeCharactersArray);
    }

    def dataframeAnalysis = {
      //START TF-IDF
      //TF
      val documents = sentPlusList.withColumn("id", monotonically_increasing_id());

      val columns = documents.columns.map(col) :+
        (explode(col("characters")) as "token")

      val unfoldedDocs = documents.select(columns: _*).cache();

      val tokensWithTf =
        unfoldedDocs
          .groupBy("id", "token")
          .agg(count("characters") as "tf");

      println(" Frodo words: " + tokensWithTf.select($"token", $"tf").where($"token" like("Frodo")).agg(sum($"tf")).first().get(0));

      //look for an ID on a column: regex ((\|([\s])+ID\|) [\s]+)

      //IDF
      val tokensWithDf =
        unfoldedDocs
          .groupBy("token")
          .agg(countDistinct("id") as "df")

      tokensWithDf.show(truncate = false);

      val dNorm = documents.count().toInt
      println(s"${dNorm} documents");

      val calcIdfUdf = udf { df: Long => Utils.calcIdf(dNorm, df toDouble) }

      val withIdf = tokensWithDf.withColumn("idf", calcIdfUdf(col("df")))
      //withIdf.show(10)


      println("Frodo words * sent: "+  tokensWithTf
        .join(documents, Seq("id"))
        .withColumn("tf_sent", $"tf" * $"sentiment")
        .select($"token", $"tf_sent")
        .where($"token" like("Frodo"))
        .agg(sum($"tf_sent")).first().get(0))


      //TF+IDF
      val tfidfs = tokensWithTf
        .join(withIdf, Seq("token")/*, "left"*/)
        .withColumn("tf_idf", $"tf" * $"idf")

      tfidfs.select($"token" as ("character"), $"tf_idf")
        .groupBy("character")
        .agg(sum("tf_idf") as "tf_idf")
        .sort($"tf_idf")
        .show()

      //tfidfs.show(10)

      //TFIDF*SENTIMENT

      val temp = tfidfs
        .join(documents, Seq("id")/*, "left"*/)
        //TODO: check if jointType = left is ok
        .withColumn("tf_idf_sent", $"tf_idf" * $"sentiment")

      //temp.show(10)

      //SUM UP
      temp
        .select($"token" as ("character"), $"tf_idf_sent")
        .groupBy("character")
        .agg(sum("tf_idf_sent") as "overall goodness")
        .sort($"overall goodness")
        .show()
    }

    dataframeAnalysis

  }
}