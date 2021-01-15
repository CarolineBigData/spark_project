name := "ibm-mytest"
version := "0.1"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.0.1" ,
    "org.apache.spark" %% "spark-core" % "3.0.1" ,
    "com.ibm.stocator" % "stocator" % "1.1.3" ,
    "com.ibm.db2" % "jcc" % "11.5.5.0" ,
    "org.slf4j" % "slf4j-api" % "1.7.2")
