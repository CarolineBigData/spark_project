package common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  /** create a spark session */

  def createSparkSession(): SparkSession ={

    logger.warn("initializing a spark session")

    val sparkConf = new SparkConf()
      .setAppName("initial Spark")
      .setMaster("local[*]")
      .set("spark.core.max","2")

    val spark = SparkSession.builder().config(sparkConf).appName("emp analysis").getOrCreate()

    logger.warn("initializing a spark session ended")

    spark

  }

  /** Set checkpoint directory */

  val spark = createSparkSession()
  spark.sparkContext.setCheckpointDir("/Users/mingzeng/IdeaProjects/ibm-mytest/src/main/scala/resources/checkpoints/")

}
