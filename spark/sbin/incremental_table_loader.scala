/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions.rand
import com.datastax.spark.connector._

import scala.collection.mutable.ListBuffer

val args = sc.getConf.get("spark.driver.args").split("\\s+")

val tablename = args(0)
val keyspacename = args(1)
val pathToParquet = args(2)
val workingFolder = args(3)

val inserts = spark.read.parquet(String.format("%s/%s",pathToParquet, "inserts"))
inserts.drop("ts").orderBy(rand()).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> tablename, "keyspace" -> keyspacename)).mode("append").save()

val updates = spark.read.parquet(String.format("%s/%s", pathToParquet, "updates"))
updates.drop("ts").orderBy(rand()).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> tablename, "keyspace" -> keyspacename)).mode("append").save()

val deletes = spark.read.parquet(String.format("%s/%s",pathToParquet, "deletes"))
val primaryKeys = deletes.drop("ts").columns

var keys=deletes.collect

keys.foreach(del => {
  var i=0
  val terms = new ListBuffer[String]()
  for (name <- primaryKeys) {
    terms.append(String.format("%s=%s", name, del.get(i).toString))
    i+=1
  }
  sc.cassandraTable(keyspacename, tablename).where(terms.mkString(" and ")).deleteFromCassandra(keyspacename, tablename)
})

sys.exit(0)