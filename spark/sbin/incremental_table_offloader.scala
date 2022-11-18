/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import com.datastax.spark.connector.cql.CassandraConnector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.reflect.io.Directory
import java.io.File

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val tablename = args(0)
val keyspacename = args(1)
val columnTs = args(2)
val targetFolder = args(3)
val nulls = args(4)

def getPrimaryKey(ks: String, tb: String): ListBuffer[String] = {
  val connector = CassandraConnector(sc.getConf)
  val pk = connector.openSession.getMetadata.getKeyspace(ks).get.getTable(tb).get.getPrimaryKey
  var pkList = new ListBuffer[String]()
  val it = pk.iterator
  while (it.hasNext) {
    pkList += (it.next.getName().toString)
  }
  pkList
}

val pkFinal = getPrimaryKey(keyspacename, tablename).toSeq

val sourceDF = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tablename, "keyspace" -> keyspacename, "writetime."+columnTs -> "ts")).load()
val fullDF = sourceDF.persist(StorageLevel.MEMORY_AND_DISK)

val snapshot = spark.read.parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename)).persist(StorageLevel.MEMORY_AND_DISK)

val cond = pkFinal.map(x => col(String.format("%s.%s", "T0", x)) === col(String.format("%s.%s", "T1", x))).reduce(_ && _)
// Detected inserts
val newInsertsDF = fullDF.as("T1").join(snapshot.drop("ts").as("T0"), cond, "leftanti")
val cntNewRows = newInsertsDF.count()
// Detected updates
val columnsIncUpdate = fullDF.columns
val newUpdatesDF = fullDF.as("T1").join(snapshot.as("T0"), cond, "inner")
  .filter($"T1.ts">$"T0.ts").selectExpr(columnsIncUpdate.map(x => String.format("%s.%s", "T1", x)): _*)
val cntUpdatedRows = newUpdatesDF.count()
// Detected deletes
val newDeletesDf = snapshot.as("T0").join(fullDF.as("T1"), cond, "leftanti")
val cntDeletedRows = newDeletesDf.count()

println(String.format("%s:%s", "New rows in the source to replicate", cntNewRows.toString))
println(String.format("%s:%s", "Updated rows in the source to replicate", cntUpdatedRows.toString))
println(String.format("%s:%s", "Deleted rows in the source to replicate", cntDeletedRows.toString))

if (!"".equals(nulls) && !"\"\"".equals(nulls)){
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val replaceNulls = mapper.readValue(nulls, classOf[Map[String, String]])
  newInsertsDF.na.fill(replaceNulls).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s/%s",targetFolder, "keyspaces/incremental/", keyspacename, tablename,"inserts"))
  newUpdatesDF.drop("ts").na.fill(replaceNulls).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s/%s",targetFolder, "keyspaces/incremental/", keyspacename, tablename,"updates"))

} else
  {
    String.format("%s/%s/%s/%s",targetFolder, "keyspaces/incremental", keyspacename, tablename)
    newInsertsDF.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s/%s",targetFolder, "keyspaces/incremental", keyspacename, tablename, "inserts"))
    newUpdatesDF.drop("ts").write.mode("overwrite").parquet(String.format("%s/%s/%s/%s/%s",targetFolder, "keyspaces/incremental", keyspacename, tablename, "updates"))
  }

newDeletesDf.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s/%s",targetFolder, "keyspaces/incremental", keyspacename, tablename, "deletes"))

if (cntNewRows>0) {
  spark.catalog.refreshByPath(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))
  var pkList = getPrimaryKey(keyspacename, tablename)
  pkList+=("ts")
  val pkWithTs = pkList.toSeq
  fullDF.selectExpr(pkWithTs.map(c => c): _*).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))
}

if (cntUpdatedRows>0 && cntNewRows == 0) {
  var pkList = getPrimaryKey(keyspacename, tablename)
  val pkWithoutTs = pkList.map(x=>col(x))
  pkList+=("ts")
  val pkWithTs = pkList.toSeq
  val dfUpdates = newUpdatesDF.selectExpr(pkWithTs.map(c => c): _*).persist(StorageLevel.MEMORY_AND_DISK)
  val dfUnionAllUpdatesAndInserts = snapshot.unionAll(dfUpdates)
  val wf = Window.partitionBy(pkWithoutTs:_*).orderBy(col("ts").desc)
  val finalSnapshot = dfUnionAllUpdatesAndInserts.withColumn("row", row_number.over(wf)).where($"row" === 1).drop("row")
  finalSnapshot.selectExpr(pkWithTs.map(c => c): _*).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp"))
  spark.catalog.refreshByPath(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))
  val dfTemp = spark.read.parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp"))
  dfTemp.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))

  val dir = new Directory(new File(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp")))
  dir.deleteRecursively()

}

if (cntDeletedRows>0) {
  var pkList = getPrimaryKey(keyspacename, tablename)
  val pkWithoutTs = pkList.map(x=>col(x))
  pkList+=("ts")
  val pkWithTs = pkList.toSeq
  val finalSnapshot = snapshot.except(newDeletesDf)
  finalSnapshot.selectExpr(pkWithTs.map(c => c): _*).write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp"))
  spark.catalog.refreshByPath(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))
  val dfTemp = spark.read.parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp"))
  dfTemp.write.mode("overwrite").parquet(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename))
  val dir = new Directory(new File(String.format("%s/%s/%s/%s",targetFolder, "keyspaces/snapshots", keyspacename, tablename+"_tmp")))
  dir.deleteRecursively()

}

sys.exit(0)