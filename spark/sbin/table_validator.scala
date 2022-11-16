/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import spark.implicits._
import scala.collection.mutable.ListBuffer
import com.datastax.spark.connector.cql.CassandraConnector

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val tablename = args(0)
val keyspacename = args(1)
val pathToParquet = args(2)
val workingFolder = args(3)

val connector = CassandraConnector(sc.getConf)
val pk = connector.openSession.getMetadata.getKeyspace(keyspacename).get.getTable(tablename).get.getPrimaryKey
var pkList = new ListBuffer[String]()
val it = pk.iterator
while (it.hasNext) { pkList += (it.next.getName().toString) }

val sourceTable = spark.read.parquet(pathToParquet)
val sourceCnt = sourceTable.count()
val targetTable = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tablename, "keyspace" -> keyspacename)).load()
val targetCnt = targetTable.selectExpr(pkList.map(c => c): _*).toJavaRDD.count()

val output = String.format("\033[31mTable %s.%s in the source: %s, in the target: %s\033[0m", keyspacename, tablename, sourceCnt.toString,targetCnt.toString)
println(output)

sys.exit(0)