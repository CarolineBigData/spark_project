import common.ConnectionJsonParser._
import common.{DataTransformation, SparkCommon, StocatorCommon, db2Common}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object EmpAnalysis {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {


    try {

      logger.warn("main method started")

      //initialize spark session
      val spark = SparkCommon.createSparkSession()

      // connect stocator
      StocatorCommon.setUpCOS(spark)

      // read data from cos bucket
      val empDF = spark.read.option("inferSchema", "true").option("header", "true").csv(fetchStocatorPath())
      empDF.show(15)

      // save data to db2
      db2Common.saveDataToDb2(empDF)

      // write data from db2
      val empJdbcDF = db2Common.writeDataFromDb2(spark).get
      empJdbcDF.show(5)
      empJdbcDF.cache()

      //calculate gender ratio in each department
      val genderRatioDF = DataTransformation.calculateGenderRatio(empJdbcDF)
      genderRatioDF.coalesce(1).checkpoint

      //calculate average salary in each department
      val avgSalaryDF = DataTransformation.avgSalary(empJdbcDF)
      avgSalaryDF.coalesce(1).checkpoint

      //calculate average salary in each department
      val genderSalaryGapDF = DataTransformation.genderSalaryGap(empJdbcDF)
      genderSalaryGapDF.coalesce(1).checkpoint

      //write genderSalaryGapDF as parquet to cos bucket
      StocatorCommon.writeDataToCos(genderSalaryGapDF)

      logger.warn("main method ended")

    } catch{

      case e: Exception =>
        logger.error("An error has occured in the main method" + e.printStackTrace())
    }







  }








}





