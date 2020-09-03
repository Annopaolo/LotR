import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


object DFAnalyzer extends  Analyzer {

  override def run(names: Seq[String], spark : SparkSession) = {
    import spark.implicits._

    val data = BookDataRetriever.getData(Utils.bookPath,
                                        Utils.pipelinePath,
                                        spark)
    val docCount = data.count().toInt
    println(s"There are ${docCount} documents to be considered.")

    //data.show(10)
    val udfSentToInt = udf(Utils.sentToInt)

    val phraseAndSentInt = data.withColumn("#sent", udfSentToInt($"sentiment"))
      .select($"#sent" as("sentiment"), $"sentence");
    //phraseAndSentInt.show(10);

    val wordList = udf(Utils.wordList);

    val sentPlusList = phraseAndSentInt.withColumn("words", wordList($"sentence"))
      .select($"words" as("characters"), $"sentiment")
      .filter(size($"characters") > 0);

    def dataframeAnalysis = {
      //START TF-IDF
      //TF
      val documents = sentPlusList.withColumn("id", monotonically_increasing_id());

      val columns = documents.columns.map(col) :+
        (explode(col("characters")) as "token")

      val unfoldedDocs = documents.select(columns: _*).cache();

      val wordsWithTf =
        unfoldedDocs
          .groupBy("id", "token")
          .agg(count("characters") as "tf");

/*
      println(" Frodo words: " + wordsWithTf.select($"token", $"tf").where($"token" like("Frodo")).agg(sum($"tf")).first().get(0));
*/

      //look for an ID on a column: regex ((\|([\s])+ID\|) [\s]+)

      //IDF
      val wordsWithDf =
        unfoldedDocs
          .groupBy("token")
          .agg(countDistinct("id") as "df")

      //wordsWithDf.show(truncate = false);

      val calcIdfUdf = udf { df: Long => Utils.calcIdf(docCount, df toDouble) }

      val withIdf = wordsWithDf.withColumn("idf", calcIdfUdf(col("df")))
      //withIdf.show(10)


/*      println("Frodo words * sent: "+  wordsWithTf
        .join(documents, Seq("id"))
        .withColumn("tf_sent", $"tf" * $"sentiment")
        .select($"token", $"tf_sent")
        .where($"token" like("Frodo"))
        .agg(sum($"tf_sent")).first().get(0))*/


      //TF+IDF
      val tfidfs = wordsWithTf
        .join(withIdf, Seq("token")/*, "left"*/)
        .withColumn("tf_idf", $"tf" * $"idf")

      val tfidf = tfidfs.select($"token" as ("character"), $"tf_idf")
        .groupBy("character")
        .agg(sum("tf_idf") as "tf_idf")
        .sort(-$"tf_idf")
        .collect()
        .map(r => (r.getString(0), r.getDouble(1)))

      //tfidfs.show(10)

      //TFIDF*SENTIMENT
      val temp = tfidfs
        .join(documents, Seq("id")/*, "left"*/)
        //TODO: check if jointType = left is ok -- no, inner join (no option on both sides) is better
        .withColumn("tf_idf_sent", $"tf_idf" * $"sentiment")

      //temp.show(10)

      //SUM UP
      val tfidfsent = temp
        .select($"token" as ("character"), $"tf_idf_sent")
        .groupBy("character")
        .agg(sum("tf_idf_sent") as "overall goodness")
        .sort(-$"overall goodness")
        .collect()
        .map(r => (r.getString(0), r.getDouble(1)))

      (tfidf, tfidfsent)
    }

    dataframeAnalysis

  }
}