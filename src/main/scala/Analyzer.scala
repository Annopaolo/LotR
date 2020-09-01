import org.apache.spark.sql.SparkSession

trait Analyzer {
  def run(names: Seq[String], spark : SparkSession): (Array[(String,Double)],Array[(String,Double)])
}
