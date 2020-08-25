import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object OldHelloWorld {

  val names = Seq("Galadriel", "Bilbo", "Frodo", "Sam", "Gandalf", "Aragorn", "Legolas", "Gimli", "Gollum", "Bombadil")

  def main2(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config("spark.driver.memory", "12G")
      .config("spark.kryoserializer.buffer.max", "200M")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    /*    val document = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val token = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normal")

    val finisher = new Finisher()
      .setInputCols("normal")

    val ngram = new NGram()
      .setN(3)
      .setInputCol("finished_normal")
      .setOutputCol("3-gram")

    val gramAssembler = new DocumentAssembler()
      .setInputCol("3-gram")
      .setOutputCol("3-grams")

    val pipeline = new Pipeline().setStages(Array(document, token, normalizer, finisher, ngram, gramAssembler))

    val testing = Seq(
      (1, "Google is a famous company"),
      (2, "Peter Parker is a super hero"),
      (3, "Meh"),
      (4, "This is really bad"),
      (5, "In a hole in the ground lived a Hobbit"),
      (6, "When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.")
    ).toDS.toDF( "_id", "text")

    val result = pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(testing)
    Benchmark.time("Time to convert and show") {result.show(truncate=false)}
    */
    val pipeline2 = PretrainedPipeline("analyze_sentiment", "en")


   /* import io.circe.generic.auto._
    import io.circe.parser

    //implicit val chapterDecoder = deriveDecoder[Chapter]

    val bookStream = this.getClass.getResourceAsStream("/LordOfTheRingsBook.json")
    val wholeBook = parser.decode[List[Chapter]](scala.io.Source.fromInputStream(bookStream).mkString)
    wholeBook match {
      case Left(err) => println(err)
      case Right(b) => {
        val allMaps = b.par.filter(chap => chap.BookName == Book("TheReturnOfTheKing"))
          .map(c => analyze(c.ChapterData, pipeline2)).seq
        val a = collection.mutable.Map.empty[String, Int]
        allMaps.foreach( m => mapSum(a,m))
        println(a)
        /*          .foreach(
          c => {
            println(c.ChapterName)
            analyze(c.ChapterData, pipeline2)
          }
        )*/
      }
    }*/

    def analyze(data: String, pip: PretrainedPipeline) = {
      val result = pip.annotate(data)

      println(result.keys)

      val sentences = result("sentence")
      val sentiment = result("sentiment")


      val m = for (i <- 0 until sentences.size) yield (sentences(i), sentiment(i))
      val store = spark.sparkContext.parallelize(m)
        .zipWithUniqueId()
        .map(x => (x._2, x._1)) //now I have ( UID, (phrase,sentiment))
        .cache()
      val sentSentences = store.map(x => (x._1, x._2._2))
      val idSentences: RDD[(Long, String)] = store.map(x => (x._1, x._2._1))
      val allWords: RDD[(Long, Array[String])] = idSentences.map(x =>
        (x._1, x._2.replaceAll("[,.!?:;]", "").split(" ")))
      val allUpperWords: RDD[(Long, Array[String])] =
        allWords.mapValues(a => a.filter(checkHeadUpper))
          .filter(x => !x._2.isEmpty)

      val allMaps = allUpperWords.map(x => (x._1, lookupWords(x._2)))
      val usefulMaps = allMaps.filter(x => !x._2.isEmpty)
      val mergedMaps = usefulMaps.map(x => (x._1, sumUp(x._2)))

      val union = mergedMaps.join(sentSentences)
      val normalizedFreqs = union.map(x => (x._1, mapSentiment(x._2._1, x._2._2)))

      val tuples = normalizedFreqs.map(x => (0, x._2))

      val freqTable = new FreqAccumulator
      spark.sparkContext.register(freqTable, "frequencies")

      tuples.foreach(x => freqTable.add(x._2))

      freqTable.value
    }

    def checkHeadUpper(s: String): Boolean = {
      s.headOption match {
        case Some(c) => if (c.isUpper) true else false
        case None => false
      }
    }

    def mapSentiment(stringToInt: Map[String, Int], sent: String) = {
      sent match {
        case "negative" => stringToInt.map(x => (x._1, -x._2))
        case "positive" => stringToInt
      }
    }

    def lookupWords(in: Array[String]): Array[(String, Int)] = {
      in.map(s => lookupWordInNames(s)).flatten
    }

    def lookupWordInNames(w: String): Option[(String, Int)] = {
      names.indexOf(w) match {
        case (-1) => None
        case _ => Some((w, 1))
      }
    }

    def sumUp(in: Array[(String, Int)]): Map[String, Int] = {
      val ret: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map.empty
      for ((k, v) <- in) {
        ret.get(k) match {
          case None => ret += (k -> v)
          case Some(x) => ret(k) = x + v
        }
      }
      ret.toMap[String, Int]
    }

    def mapSum(in1: collection.mutable.Map[String, Int], in2: Map[String, Int]) = {
      for ((name, value) <- in2) {
        in1.get(name) match {
          case None => in1 += (name -> value)
          case Some(x) => in1(name) = x + value
        }
      }
      in1
    }

    def printSentiment(df:DataFrame, pipeline: PretrainedPipeline) = {
      val analysis = pipeline.transform(df)
      analysis.select("sentiment").show(10,false)
    }

    def goodData(s:String): Seq[(Int,String)] = {
      s.lines.zipWithIndex.toSeq.map(s => (s._2,s._1))
    }

  }


}