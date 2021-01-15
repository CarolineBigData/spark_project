import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait spark {

  // initial spark
  val sparkConf = new SparkConf()
    .setAppName("initial Spark")
    .setMaster("local[*]")
    .set("spark.core.max","2")

  val spark = SparkSession.builder().config(sparkConf).appName("emp analysis").getOrCreate()

}