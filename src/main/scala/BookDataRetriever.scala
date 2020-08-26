import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, col, explode}

object BookDataRetriever {

    def getData(bookPath : String, pipelinePath : String, spark : SparkSession): DataFrame ={

      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")

      val book = spark.read.option("multiline", "true").json(bookPath).cache()
      //book.printSchema()
      val phrases = book.select("ChapterData")
      //val pipeline2 = PretrainedPipeline("analyze_sentiment", "en")
      val pipeline2 = PretrainedPipeline.fromDisk(pipelinePath)

      val annotation = pipeline2.annotate(phrases, "ChapterData")
      //trans.show(10);
      //trans.printSchema()

      val columns = annotation.select(
        col("sentiment.result").as("sentiment"),
        col("sentence.result").as("sentence")
        //, col("token.result").as("tokens")
      )

      columns.
        withColumn("r", explode(arrays_zip($"sentiment", $"sentence")))
        .select(
          $"r.sentiment" as "sentiment",
          $"r.sentence" as "sentence")

    }

}
