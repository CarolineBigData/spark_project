import common.DataTransformation.getClass
import common.{DataTransformation, SparkCommon}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._

import scala.common.SparkBase


class SparkTransformationSpec extends SparkBase {

  val sparkConf = new SparkConf()
    .setAppName("initial Spark")
    .setMaster("local[*]")
    .set("spark.core.max", "2")

  val spark = SparkSession.builder().config(sparkConf).appName("emp analysis").getOrCreate()

  def fixture = new {
    val df : DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("src/main/test/scala/resources/mock_emp_data")
  }


  behavior of "spark transformation"

  /** test of calculateGenderRatio method */
  it should "calculate gender ratio in each department" in {

    val f = fixture

    val genderRatioDF = DataTransformation.calculateGenderRatio(f.df)
    genderRatioDF.show(2)

    val engGenderRatio = genderRatioDF
      .filter(genderRatioDF("Department") === "Engineering")
      .select("genderRate")
      .collect()

    val engGR = engGenderRatio(0)(0)

    assert( 1.0 == engGR)


  }

  /** test of avgSalary method */
  it should "calculate average salary in each department" in {

    val f = fixture

    val avgSalaryDF = DataTransformation.avgSalary(f.df)

    val engAvgSalary = avgSalaryDF
      .filter(avgSalaryDF("Department") === "Engineering")
      .select("AvgSalary")
      .collect()

    val engAvgSal = engAvgSalary(0)(0)

    assert( 55.0 == engAvgSal)

  }

  /** test of genderSalaryGap method */
  it should "calculate male and female salary gap in each department" in {

    val f = fixture

    val genderGapDF = DataTransformation.genderSalaryGap(f.df)
    val engGenderGap = genderGapDF
      .filter(genderGapDF("Department") === "Engineering")
      .select("GenderSalaryGap")
      .collect()

    val engGenderSalGap = engGenderGap(0)(0)

    assert( -90.0 == engGenderSalGap)

  }





  }


