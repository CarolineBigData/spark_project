package common

import org.slf4j.LoggerFactory
import com.typesafe.config._

object ConnectionJsonParser {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  /** read the Json file which stores the db2 connections  */

  def readDB2JsonFile(): Config ={

    ConfigFactory.load("db2_connections.json")

  }

  /** fetch details in db2 connections */

  def fetchDb2Url(): String={
    val url = readDB2JsonFile.getString("body.url")
    url
  }

  def fetchDb2Usr(): String={
    val usr = readDB2JsonFile.getString("body.usr")
    usr
  }

  def fetchDb2Pwd(): String={
    val pwd = readDB2JsonFile.getString("body.pwd")
    pwd
  }

  def fetchDb2Dbtable(): String={
    val dbtable = readDB2JsonFile.getString("body.dbtable")
    dbtable
  }

  def fetchDb2DbdbDriverClasse(): String={
    val db2DriverClass = readDB2JsonFile.getString("body.db2DriverClass")
    db2DriverClass
  }


  /** read the Json file which stores the stocator connections  */

  def readStocatorJsonFile(): Config ={

    ConfigFactory.load("stocator_connection.json")

  }
  /** fetch details in Stocator connections */

  def fetchStocatorPath(): String={
    val path = readStocatorJsonFile().getString("body.path") //key ="body.pg_target_table"
    path
  }

  def fetchStocatorEndpoint(): String={
    val endpoint= readStocatorJsonFile().getString("body.endpoint")
    endpoint
  }

  def fetchStocatorAccessKey(): String={
    val accessKey = readStocatorJsonFile().getString("body.accessKey")
    accessKey
  }

  def fetchStocatorSecretkey(): String={
    val secretKey = readStocatorJsonFile().getString("body.secretKey")
    secretKey
  }












}
