package common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._


object DataTransformation {

  val spark = SparkCommon.createSparkSession()

  private val logger = LoggerFactory.getLogger(getClass.getName)

  /** check null values in  data */
  def checkGenderNull(df: DataFrame): DataFrame = {

    val genderCheckNull = df.select("Gender")
      .withColumn("gender_is_null", col("Gender").isNull)
      .filter("gender_is_null == True")


    genderCheckNull

  }

  /** calculate gender ratio in each department */
  def calculateGenderRatio(df: DataFrame): DataFrame = {

    logger.warn("calculating gender ratio started")


    // Use Encoders StringToColumn method to convert $"name" into a Column
    import spark.implicits._

    //calculate gender ratio in each department
    val genderRatio = df.filter(col("Gender").isNotNull)
      .groupBy("Department")
      .agg(round((sum(when($"gender" === "Male", lit(1))) / sum(when($"gender" === "Female", lit(1)))),2).alias("genderRate"))

    genderRatio.show(10)

    logger.warn("calculating gender ratio ended")

    genderRatio

  }


  /** calculate average salary in each department */
  def avgSalary(df: DataFrame): DataFrame = {

    logger.warn("calculating average salary started")

    import spark.implicits._

    val avgSalary = df.withColumn("salary", regexp_replace(col("Salary"), "[$,]", "").cast(DoubleType))
      .groupBy("Department")
      .agg(round(avg("salary"),2).alias("AvgSalary"))

    avgSalary.show(10)

    logger.warn("calculating average salary ended")
    avgSalary

  }


  /**  calculate Male and female salary gap in each department */
  def genderSalaryGap(df: DataFrame): DataFrame = {

    logger.warn("calculating male and female salary gap started")

    import spark.implicits._

    val genderSalaryGap = df.filter(col("Gender").isNotNull)
      .withColumn("salary", regexp_replace(col("Salary"), "[$,]", "").cast(DoubleType))
      .groupBy("Department")
      .agg(round((sum(when($"gender" === "Male", $"Salary")) - sum(when($"gender" === "Female", $"Salary"))),2).as("GenderSalaryGap"))

    genderSalaryGap.show(10)

    logger.warn("calculating male and female salary gap ended")

    genderSalaryGap

  }

}
