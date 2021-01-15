
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}


object EmployeeAnalysis extends spark {

  def main(args: Array[String]) {

    Utilities.setupLogging()

    Utilities.setUpCOS(spark)

    /** read emp-data from cos bucket and display first 15 rows of the DataFrame */

      val empDF = spark.read.option("inferSchema", "true").option("header", "true").csv(Connections.path)
      empDF.show(15)


    /** save emp data to db2 */

    empDF.write.format("jdbc")
      .mode("overwrite")
      .option("url", Connections.url)
      .option("user", Connections.usr)
      .option("password", Connections.pwd)
      .option("dbtable", Connections.dbtable)
      .option("driver", Connections.db2DriverClass)
      .save()

    /** read emp data from db2 */

    val empJdbcDF =  spark.read.format("jdbc")
      .format("jdbc")
      .option("url", Connections.url)
      .option("user", Connections.usr)
      .option("password", Connections.pwd)
      .option("dbtable", Connections.dbtable)
      .option("driver", Connections.db2DriverClass)
      .load()


    empJdbcDF.show(5)
    empJdbcDF.cache()


    /** check null values in emp data */

    def checkGenderNull(df: DataFrame): DataFrame = {

      val genderCheckNull = df.select("Gender")
      .withColumn("gender_is_null", col("Gender").isNull)
      .filter("gender_is_null == True")

      return genderCheckNull
    }



    /** calculate gender ratio in each department */

    def calculateGenderRatio(df: DataFrame): DataFrame = {

      // Use Encoders StringToColumn method to convert $"name" into a Column
      import spark.implicits._

      //calculate gender ratio in each department
      val genderRatio = df.filter(col("Gender").isNotNull)
        .groupBy("Department")
        .agg(round((sum(when($"gender" === "Male", lit(1))) / sum(when($"gender" === "Female", lit(1)))),2).alias("genderRate"))

      genderRatio.show(10)
      return genderRatio

    }


    val genderRatioDF = calculateGenderRatio(empJdbcDF)
    genderRatioDF.coalesce(1).checkpoint


    /** calculate average salary in each department */


    def avgSalary(df: DataFrame): DataFrame = {

      val avgSalary = df.withColumn("salary", regexp_replace(col("Salary"), "[$,]", "").cast(DoubleType))
        .groupBy("Department")
        .agg(round(avg("salary"),2).alias("AvgSalary"))


      avgSalary.show(10)
      return avgSalary

    }

    val avgSalaryDF = avgSalary(empJdbcDF)
    avgSalaryDF.coalesce(1).checkpoint


    /**  Male and female salary gap in each department */

    def genderSalaryGap(df: DataFrame): DataFrame = {

      import spark.implicits._

      val genderSalaryGap = df.filter(col("Gender").isNotNull)
        .withColumn("salary", regexp_replace(col("Salary"), "[$,]", "").cast(DoubleType))
        .groupBy("Department")
        .agg(round((sum(when($"gender" === "Male", $"Salary")) - sum(when($"gender" === "Female", $"Salary"))),2).as("GenderSalaryGap"))

      genderSalaryGap.show(10)

      return genderSalaryGap

    }

    val genderSalaryGapDF = genderSalaryGap(empJdbcDF)
    genderSalaryGapDF.coalesce(1).checkpoint


    /** write genderSalaryGapDF as parquet to cos bucket*/

    val genderSalaryGapPath = "cos://candidate-exercise.myCos/genderSalaryGap.parquet"

    genderSalaryGapDF
      .write
      .mode("overwrite")
      .parquet(genderSalaryGapPath)

    spark.close()



  }

}

