object Connections {

  val accessKey = "0aba66146f3b450cacebaa908046d17e"
  val secretKey = "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd"
  val endpoint = "https://s3.us.cloud-object-storage.appdomain.cloud"

  val service = "myCos"
  val bucketName = "candidate-exercise"
  val path = s"cos://${bucketName}.${service}/emp-data.csv"

  val db2DriverClass = "com.ibm.db2.jcc.DB2Driver"
  val usr = "vzh47955"
  val pwd = "z2pt9-2w79b7lch0"
  val url = "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB"
  val dbtable = "vzh47955.emptable"


}
