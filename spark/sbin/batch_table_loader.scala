/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.cassandra._
import spark.implicits._
import org.apache.spark.sql.functions.rand

val args = sc.getConf.get("spark.driver.args").split("\\s+")

val tablename = args(0)
val keyspacename = args(1)
val pathToParquet = args(2)
val workingFolder = args(3)

val sourceTable = spark.read.parquet(pathToParquet)
sourceTable.drop("ts").orderBy(rand()).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> tablename, "keyspace" -> keyspacename))
  .mode("append")
  .save()

sys.exit(0)