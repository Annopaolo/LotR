import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


object DFAnalyzer extends  Analyzer {

  override def run(names: Seq[String], spark : SparkSession): Unit = {
    val phr = BookDataRetriever.getData(Utils.bookPath,
                                        Utils.pipelinePath,
                                        spark)
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //phr.show(10)
    val udfSentToInt = udf(Utils.sentToInt)

    val end = phr.withColumn("#sent", udfSentToInt($"sentiment"))
      .select($"#sent" as("sentiment"), $"sentence");
    //end.show(10);

    val wordList = udf(Utils.wordList);

    val sentPlusList = end.withColumn("words", wordList($"sentence"))
      .select($"words" as("characters"), $"sentiment")
      .filter(size($"characters") > 0);

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
        .sort(-$"tf_idf")
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
        .sort(-$"overall goodness")
        .show()
    }

    dataframeAnalysis

  }
}