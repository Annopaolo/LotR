import org.apache.spark.sql.SparkSession

trait Analyzer {
  def run(names: Seq[String], spark : SparkSession): Unit
}
