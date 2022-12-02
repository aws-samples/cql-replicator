/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import scala.collection.mutable.ListBuffer
import com.datastax.spark.connector.cql.CassandraConnector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val tablename = args(0)
val keyspacename = args(1)
val columnTs = args(2)
val targetFolder = args(3)
val nulls = args(4)

val connector = CassandraConnector(sc.getConf)
val pk = connector.openSession.getMetadata.getKeyspace(keyspacename).get.getTable(tablename).get.getPrimaryKey
var pkList = new ListBuffer[String]()
val it = pk.iterator
while (it.hasNext) { pkList += (it.next.getName().toString) }
pkList += "ts"

var pkFinal = pkList.toSeq

if (columnTs == "NA") {
  val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tablename, "keyspace" -> keyspacename)).load()
  val cnt = df.count()
  var pkFinalNoTs = pkFinal.filter(_ != "ts")

  df.selectExpr(pkFinalNoTs.map(c => c): _*).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))

  if (!"".equals(nulls) && !"\"\"".equals(nulls) ) {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val replaceNulls = mapper.readValue(nulls, classOf[Map[String, String]])

    df.na.fill(replaceNulls).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces", keyspacename, tablename))
  } else
    {
      df.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces", keyspacename, tablename))
    }

} else {
  val df = spark.read.cassandraFormat(tablename,keyspacename).options(Map("writetime."+columnTs->"ts")).load()
  val cnt = df.count()
  println("Number of rows in the source table:" + cnt.toString)
  val maxTs = df.selectExpr("max(ts)").collect()(0).get(0)
  println("Use this timestamp for CQLReplicator config.properties:" + maxTs)

  df.selectExpr(pkFinal.map(c=>c): _*).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))

  if (!"".equals(nulls) && !"\"\"".equals(nulls)) {
    val mapper = new ObjectMapper()//  with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val replaceNulls = mapper.readValue(nulls, classOf[Map[String, String]])

    df.na.fill(replaceNulls).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces", keyspacename, tablename))
  } else {
    df.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces", keyspacename, tablename))
  }
}
sys.exit(0)