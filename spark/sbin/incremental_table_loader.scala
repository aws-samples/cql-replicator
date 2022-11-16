/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import spark.implicits._
import org.apache.spark.sql.functions.rand
import com.datastax.spark.connector._

val args = sc.getConf.get("spark.driver.args").split("\\s+")

val tablename = args(0)
val keyspacename = args(1)
val pathToParquet = args(2)
val workingFolder = args(3)

val inserts = spark.read.parquet(pathToParquet+"/"+"inserts")
inserts.drop("ts").orderBy(rand()).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> tablename, "keyspace" -> keyspacename)).mode("append").save()

val updates = spark.read.parquet(pathToParquet+"/"+"updates")
updates.drop("ts").orderBy(rand()).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> tablename, "keyspace" -> keyspacename)).mode("append").save()

val deletes = spark.read.parquet(pathToParquet+"/"+"deletes")
val primaryKeys = deletes.drop("ts").columns

deletes.rdd.foreach(del => {
  val whereClause = String.format("key='%s' and col0=%s", del.get(0).toString, del.get(1).toString)
  sc.cassandraTable(keyspacename, tablename).where(whereClause).deleteFromCassandra(keyspacename, tablename)
})

sys.exit(0)