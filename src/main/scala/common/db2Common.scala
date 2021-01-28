package common

import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object db2Common {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def saveDataToDb2(df: DataFrame): Unit ={

    try {

      logger.warn("saving data to db2 started")

      df.write.format("jdbc")
        .mode("overwrite")
        .option("url", ConnectionJsonParser.fetchDb2Url())
        .option("user", ConnectionJsonParser.fetchDb2Usr())
        .option("password", ConnectionJsonParser.fetchDb2Pwd())
        .option("dbtable", ConnectionJsonParser.fetchDb2Dbtable())
        .option("driver", ConnectionJsonParser.fetchDb2DbdbDriverClasse())
        .save()

      logger.warn("saving data to db2 ended")
    } catch{
      case e: Exception =>
        logger.error("There is an error in saving data to db2")
    }

  }

  def writeDataFromDb2(spark: SparkSession) : Option[DataFrame] ={

    try {

      logger.warn("writing data from db2 started")

      val empJdbcDF = spark.read.format("jdbc")
        .format("jdbc")
        .option("url", ConnectionJsonParser.fetchDb2Url())
        .option("user", ConnectionJsonParser.fetchDb2Usr())
        .option("password", ConnectionJsonParser.fetchDb2Pwd())
        .option("dbtable", ConnectionJsonParser.fetchDb2Dbtable())
        .option("driver", ConnectionJsonParser.fetchDb2DbdbDriverClasse())
        .load()

      logger.warn("writing data from db2 ended")
      Some(empJdbcDF)
    }catch{
      case e: Exception =>
        logger.error("There is an error in writing data from db2")
        None
    }
  }

}
