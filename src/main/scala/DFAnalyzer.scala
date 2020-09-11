import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DFAnalyzer extends Analyzer {

  override def run(names: Seq[String], spark : SparkSession) = {
    import spark.implicits._

    val data = BookDataRetriever.getData(Utils.bookPath,
                                        Utils.pipelinePath,
                                        spark)
    val docCount = data.count().toInt
    println(s"There are ${docCount} documents to be considered.")

    val udfSentToInt = udf(Utils.sentToInt)
    val phraseAndSentInt = data.withColumn("#sent", udfSentToInt($"sentiment"))
      .select($"#sent" as("sentiment"), $"sentence");

    val wordList = udf(Utils.wordList);
    val sentPlusList = phraseAndSentInt.withColumn("words", wordList($"sentence"))
      .select($"words" as("characters"), $"sentiment")
      .filter(size($"characters") > 0);

    def dataframeAnalysis = {
      //TF
      val documents = sentPlusList.withColumn("id", monotonically_increasing_id());

      val columns = documents.columns.map(col) :+
        (explode(col("characters")) as "token")

      val unfoldedDocs = documents.select(columns: _*).cache();

      val wordsWithTf =
        unfoldedDocs
          .groupBy("id", "token")
          .agg(count("characters") as "tf");

      //IDF
      val wordsWithDf =
        unfoldedDocs
          .groupBy("token")
          .agg(countDistinct("id") as "df")

      val calcIdfUdf = udf { df: Long => Utils.calcIdf(docCount, df toDouble) }
      val withIdf = wordsWithDf.withColumn("idf", calcIdfUdf(col("df")))

      def computeResult(in: DataFrame, selectedColumn: String, computationName: String) : Array[(String, Double)] = {
        in.select($"token" as ("character"), col(selectedColumn))
          .groupBy("character")
          .agg(sum(col(selectedColumn)) as computationName)
          .collect()
          .map(r => (r.getString(0), r.getDouble(1)))
      }

      //TF+IDF
      val dfForTfIdf= wordsWithTf
        .join(withIdf, Seq("token")/*, "left"*/)
        .withColumn("tf_idf", $"tf" * $"idf")
      val tfidf = computeResult(dfForTfIdf, "tf_idf", "tf-idf")

      //TFIDF*SENTIMENT
      val dfForTFIdfSent = dfForTfIdf
        .join(documents, Seq("id")/*, "left"*/)
        //inner join is better
        .withColumn("tf_idf_sent", $"tf_idf" * $"sentiment")
      val tfidfsent = computeResult(dfForTFIdfSent, "tf_idf_sent", "tf-idf * sentiment")

      (tfidf, tfidfsent)
    }

    dataframeAnalysis

  }
}