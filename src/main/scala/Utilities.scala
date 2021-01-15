import java.util.regex.Pattern

import EmployeeAnalysis.spark
import org.apache.spark.sql.SparkSession

object Utilities {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Set checkpoint directory */
  spark.sparkContext.setCheckpointDir("/Users/mingzeng/IdeaProjects/ibm-mytest/src/main/scala/resources/checkpoints/")

  /** Configure credentials */
  def setUpCOS(spark: SparkSession): Unit = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    /** Configure stocator with template */
    hadoopConf.set("fs.stocator.scheme.list", "cos")
    hadoopConf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    hadoopConf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    hadoopConf.set("fs.stocator.cos.scheme", "cos")

    /** configure Stocator with COS credentials*/
    hadoopConf.set(s"fs.cos.${Connections.service}.endpoint", Connections.endpoint)
    hadoopConf.set(s"fs.cos.${Connections.service}.access.key", Connections.accessKey)
    hadoopConf.set(s"fs.cos.${Connections.service}.secret.key", Connections.secretKey)

  }

}