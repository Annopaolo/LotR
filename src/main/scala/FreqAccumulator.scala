import org.apache.spark.util.AccumulatorV2

class FreqAccumulator extends AccumulatorV2[Map[String, Int], Map[String,Int]]  {

  private var freqs = scala.collection.mutable.Map.empty[String, Int]

  override def isZero: Boolean = freqs.isEmpty

  override def copy(): AccumulatorV2[Map[String, Int], Map[String, Int]] = {
    var ret = new FreqAccumulator
    ret.freqs = freqs.clone()
    ret
  }

  override def reset(): Unit = {
    freqs = scala.collection.mutable.Map.empty[String, Int]
  }

  override def add(v: Map[String, Int]): Unit = {
    for ( (name,value) <- v) {
      freqs.get(name) match {
        case None => freqs += (name -> value)
        case Some(x) => freqs(name) = x+value
      }
    }
  }

  override def merge(other: AccumulatorV2[Map[String, Int], Map[String, Int]]): Unit = {
      this.add(other.value)
  }

  override def value: Map[String, Int] = freqs.toMap[String, Int]
}
