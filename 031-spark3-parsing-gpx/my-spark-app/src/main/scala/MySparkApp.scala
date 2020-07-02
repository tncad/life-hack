import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MySparkApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("my-spark-app")
    val sc = new SparkContext(conf)
    println("Hello Spark!")
  }
}
