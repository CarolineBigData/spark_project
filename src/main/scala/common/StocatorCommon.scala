package common

import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object StocatorCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def setUpCOS(spark: SparkSession): Unit = {

    logger.warn("connecting stocator started")

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    /** Configure stocator with template */

    hadoopConf.set("fs.stocator.scheme.list", "cos")
    hadoopConf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    hadoopConf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    hadoopConf.set("fs.stocator.cos.scheme", "cos")

    /** configure Stocator with COS credentials*/

    hadoopConf.set(s"fs.cos.myCos.endpoint", ConnectionJsonParser.fetchStocatorEndpoint())
    hadoopConf.set(s"fs.cos.myCos.access.key", ConnectionJsonParser.fetchStocatorAccessKey())
    hadoopConf.set(s"fs.cos.myCos.secret.key", ConnectionJsonParser.fetchStocatorSecretkey())

    logger.warn("connecting stocator ended")
  }


  def writeDataToCos(df: DataFrame): Unit ={

    try {

      logger.warn("writing data to cos bucket started")

      val genderSalaryGapPath = "cos://candidate-exercise.myCos/genderSalaryGap.parquet"

      df.write.mode("overwrite").parquet(genderSalaryGapPath)

      logger.warn("writing data to cos bucket ended")
    } catch{
      case e: Exception =>
        logger.error("There is an error in writing the data to cos bucket")
    }



  }


}
