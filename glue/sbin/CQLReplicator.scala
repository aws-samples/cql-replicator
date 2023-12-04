/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.spark.sql.cassandra._
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.time.Duration

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector._
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.servererrors._

import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.github.resilience4j.core.IntervalFunction

import scala.util.{Try, Success, Failure}

import scala.collection.mutable.StringBuilder

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Base64
import java.nio.charset.StandardCharsets

class LargeObjectException(s: String) extends Exception(s) {}

object GlueApp {
  def main(sysArgs: Array[String]) {
    def shuffleDf(df: DataFrame): DataFrame = {
      val encoder = RowEncoder(df.schema)
      df.mapPartitions(new scala.util.Random().shuffle(_))(encoder)
    }

    def customConnectionFactory(sc: SparkContext): (CassandraConnector, CassandraConnector) = {
      val connectorToClusterSrc = CassandraConnector(sc.getConf.set("spark.cassandra.connection.config.profile.path", "KeyspacesConnector.conf"))
      val connectorToClusterTrg = CassandraConnector(sc.getConf.set("spark.cassandra.connection.config.profile.path", "CassandraConnector.conf"))
      (connectorToClusterSrc, connectorToClusterTrg)
    }

    def inferKeys(cc: CassandraConnector, keyType: String, ks: String, tbl: String, columnTs: String): Seq[Map[String, String]] = {
      val meta = cc.openSession.getMetadata.getKeyspace(ks).get.getTable(tbl).get
      keyType match {
        case "partitionKeys" => {
          meta.getPartitionKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
        case "primaryKeys" => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
        case "primaryKeysWithTS" => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase)) :+ Map(s"writetime($columnTs) as ts" -> "bigint")
        }
        case _ => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
      }
    }

    def parseJSONConfig(i: String): org.json4s.JValue = i match {
      case "None" => JObject(List(("None",JString("None"))))
      case str =>
        try {
          parse(str)
        } catch {
          case _: Throwable => JObject(List(("None",JString("None"))))
        }
    }

    val WAIT_TIME = 10000
    val MAX_RETRY_ATTEMPTS = 256
    // Unit ms
    val EXP_BACKOFF= 25
    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val sparkConf: SparkConf = sparkContext.getConf
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "TILE", "TOTAL_TILES", "PROCESS_TYPE", "SOURCE_KS", "SOURCE_TBL", "TARGET_KS", "TARGET_TBL", "WRITETIME_COLUMN", "TTL_COLUMN", "S3_LANDING_ZONE", "OFFLOAD_LARGE_OBJECTS").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val jobRunId = args("JOB_RUN_ID")
    val currentTile = args("TILE").toInt
    val totalTiles = args("TOTAL_TILES").toInt
    // Internal configuration
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog.spark.cassandra.connection.config.profile.path", "KeyspacesConnector.conf")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster.spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")

    val ledgerTable = "ledger"
    val ledgerKeyspaces = "migration"
    val processType = args("PROCESS_TYPE") // discovery or replication
    val interanlLedger = s"ledgerCatalog.$ledgerKeyspaces.$ledgerTable"

    // Business configuration
    val srcTableName = args("SOURCE_TBL")
    val srcKeyspaceName = args("SOURCE_KS")
    val trgTableName = args("TARGET_TBL")
    val trgKeyspaceName = args("TARGET_KS")
    val customConnections = customConnectionFactory(sparkContext)
    val cassandraConn = customConnections._2
    val keyspacesConn = customConnections._1
    val landingZone = args("S3_LANDING_ZONE")
    val columnTs = args("WRITETIME_COLUMN")
    val source = s"sourceCluster.$srcKeyspaceName.$srcTableName"
    val ttlColumn = args("TTL_COLUMN")
    val olo = args("OFFLOAD_LARGE_OBJECTS")
    val selectStmtWithTTL = ttlColumn match {
      case "None" => ""
      case _ => {
        val tmpMeta = cassandraConn.openSession.getMetadata.getKeyspace(srcKeyspaceName).get.getTable(srcTableName).get
        val lstColumns = tmpMeta.getColumns.asScala.map(x => x._1.toString).toSeq :+ s"ttl($ttlColumn)"
        lstColumns.mkString(",")
      }
    }

    val pkFinal = columnTs match {
      case "None" => inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
      case _ => inferKeys(cassandraConn, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    }

    val pkFinalWithoutTs = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val pks = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex

    val offloadLageObjTmp = new String(Base64.getDecoder().decode(olo.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    logger.info(s"Offload large objects to S3 bucket: $offloadLageObjTmp")

    val offloadLargeObjects = parseJSONConfig(offloadLageObjTmp)

    //AmazonS3Client to check if a stop requested issued
    val s3client = new AmazonS3Client()

    def stopRequested(bucket: String, key: String): Boolean = {
      Try {
        val s3Object: S3Object = s3client.getObject(bucket, key)
        s3client.deleteObject(bucket, key)
        logger.info(s"Requested a stop for $key process")
      } match {
        case Failure(_) => false
        case Success(_) => true
      }
    }

    def offloadToS3(jPayload: org.json4s.JValue, s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3): JValue = {
      val columnName = (offloadLargeObjects \ "column").values.toString
      val bucket = (offloadLargeObjects \ "bucket").values.toString
      val prefix = (offloadLargeObjects \ "prefix").values.toString
      val xrefColumnName = (offloadLargeObjects \ "xref").values.toString
      val key = java.util.UUID.randomUUID.toString
      val largeObject = (jPayload \ columnName).values.toString
      val jsonStatement = jPayload transformField {
        case JField(`xrefColumnName`, _) => JField(xrefColumnName, org.json4s.JsonAST.JString(s"s3://$bucket/$prefix/$key"))
      }
      // Remove orginal payload from the target statement
      val updatedJsonStatement = jsonStatement removeField {
        case JField(`columnName`, _) => true
        case _ => false
      }

      Try {
        s3ClientOnPartition.putObject(bucket, s"$prefix/$key", largeObject)
      } match {
        case Failure(_) => throw new LargeObjectException("Not able to persist the large object to S3")
        case Success(_) => updatedJsonStatement
      }
    }

    def putStats(bucket: String, key: String, metric: String, value: String): Unit = {
      s3client.putObject(bucket, s"$key/$metric", value)
    }

    def getTTLvalue(jvalue: org.json4s.JValue): BigInt = {
      val jvalueByKey = jvalue \ s"ttl($ttlColumn)"
      jvalueByKey match {
        case JInt(values) => values
        case _ => 0
      }
    }

    def backToCQLStatementWithoutTTL(jvalue: org.json4s.JValue): String = {
      val res = jvalue filterField (p => (p._1 != s"ttl($ttlColumn)"))
      val jsonNew = JObject(res)
      compact(render(jsonNew))
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, ver: String): Unit = {
      df.rdd.foreachPartition(
        partition => {
          val retryConfig = RetryConfig.custom.maxAttempts(MAX_RETRY_ATTEMPTS).
            intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(EXP_BACKOFF), 1.1)).
            retryExceptions(classOf[WriteFailureException],classOf[WriteTimeoutException],classOf[ServerError],classOf[UnavailableException],classOf[NoNodeAvailableException], classOf[AllNodesFailedException]).build()
          val retry = Retry.of("keyspaces", retryConfig)
          val s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3 = offloadLargeObjects match {
            case JObject(List(("None",JString("None")))) => null
            case _ => AmazonS3ClientBuilder.defaultClient()
          }
          /* This section only for debugging
          val publisher = retry.getEventPublisher
          publisher.onRetry(event => logger.info(s"Operation was retried on event $event"))
          publisher.onError(event => logger.info(s"Operation was failed on event $event"))
          */

          partition.foreach(
            row => {
              val whereClause = rowToStatement(row, columns, columnsPos)

              if (!whereClause.isEmpty) {
                cassandraConn.withSessionDo { session => {
                  if (ttlColumn.equals("None")) {
                    val rs = Option(session.execute(s"SELECT json * FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                    if (!rs.isEmpty) {
                      val jsonRow = rs.get.getString(0).replace("'", "\\\\u0027")
                      keyspacesConn.withSessionDo {
                        session => {
                          offloadLargeObjects match {
                            case JObject(List(("None",JString("None")))) => Retry.decorateSupplier(retry,
                              () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$jsonRow'")).get()
                            case _ => {
                              val json4sRow = parse(jsonRow)
                              val updatedJsonRow = compact(render(offloadToS3(json4sRow, s3ClientOnPartition)))
                              Retry.decorateSupplier(retry,
                                () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$updatedJsonRow'")).get()
                            }
                          }

                        }
                      }
                    }
                  }
                  else {
                    val rs = Option(session.execute(s"SELECT json $selectStmtWithTTL FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                    if (!rs.isEmpty) {
                      val jsonRow = rs.get.getString(0).replace("'", "\\\\u0027")
                      val json4sRow = parse(jsonRow)
                      keyspacesConn.withSessionDo {
                        session => {
                          offloadLargeObjects match {
                            case JObject(List(("None",JString("None")))) => {
                              val backToJsonRow = backToCQLStatementWithoutTTL(json4sRow)
                              val ttlVal = getTTLvalue(json4sRow)
                              Retry.decorateSupplier(retry, () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal")).get()
                            }
                            case _ => {
                              val json4sRow = parse(jsonRow)
                              val updatedJsonRow = offloadToS3(json4sRow, s3ClientOnPartition)
                              val backToJsonRow = backToCQLStatementWithoutTTL(updatedJsonRow)
                              val ttlVal = getTTLvalue(json4sRow)
                              Retry.decorateSupplier(retry, () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal")).get()
                            }
                          }
                        }
                      }
                    }
                  }
                }
                }
              }
            }
          )
        }
      )
    }

    def rowToStatement(row: Row, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)]): String = {
      val whereStmt = new StringBuilder
      columnsPos.foreach(
        el => {
          val colName = el._1
          val position = row.fieldIndex(el._1)
          val colType: String = columns.getOrElse(el._1, "can't detect data type")
          colType match {
            case "string" => {
              val v = row.getString(position);
              whereStmt.append(s"$colName='$v'")
            }
            case "text" => {
              val v = row.getString(position);
              whereStmt.append(s"$colName='$v'")
            }
            case "int" => {
              val v = row.getInt(position);
              whereStmt.append(s"$colName=$v")
            }
            case "long" => {
              val v = row.getLong(position);
              whereStmt.append(s"$colName=$v")
            }
            case "bigint" => {
              val v = row.getLong(position);
              whereStmt.append(s"$colName=$v")
            }
            case "float" => {
              val v = row.getFloat(position);
              whereStmt.append(s"$colName=$v")
            }
            case "date" => {
              val v = row.getDate(position);
              whereStmt.append(s"$colName='$v'")
            }
            case "double" => {
              val v = row.getDouble(position);
              whereStmt.append(s"$colName=$v")
            }
            case "timestamp" => {
              val v = row.getTimestamp(position);
              whereStmt.append(s"$colName='$v'")
            }
            case "short" => {
              val v = row.getShort(position);
              whereStmt.append(s"$colName=$v")
            }
            case "decimal" => {
              val v = row.getDecimal(position);
              whereStmt.append(s"$colName=$v")
            }
            case "tinyint" => {
              val v = row.getByte(position);
              whereStmt.append(s"$colName=$v")
            }
            case "uuid" => {
              val v = row.getString(position);
              whereStmt.append(s"$colName=$v")
            }
            case _ => {
              val v = row.getString(position);
              whereStmt.append(s"$colName='$v'")
            }
          }
          if (el._2 < columns.size - 1) {
            whereStmt.append(" and ")
          }
        })
      whereStmt.toString
    }

    def dataReplicationProcess() {
      keyspacesConn.withSessionDo {
        session => {
          val ledger = session.execute(s"SELECT location,tile,ver FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and load_status='' and offload_status='SUCCESS' ALLOW FILTERING").all().asScala
          val ledgerList = Option(ledger)

          if (!ledgerList.isEmpty) {
            val locations = ledgerList.get.map(c => (c.getString(0), c.getInt(1), c.getString(2))).toList.par
            val heads = locations.filter(_._3 == "head").length
            val tails = locations.filter(_._3 == "tail").length

            if (heads > 0 && tails == 0) {

              logger.info(s"Historical data load.Processing locations: $locations")
              locations.foreach(location => {

                val loc = location._1
                val sourceDf = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/$loc")
                val sourceDfV2 = sourceDf.drop("group").drop("ts")
                val tile = location._2
                val ver = location._3

                persistToTarget(shuffleDf(sourceDfV2), columns, columnsPos, tile, ver)
                keyspacesConn.withSessionDo {
                  session => session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'$ver','SUCCESS', toTimestamp(now()), '')")
                }

                val cnt = sourceDfV2.count()
                val content = s"""{"tile":$tile, "primaryKeys":$cnt}"""
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)

              }
              )
            }

            if ((heads > 0 && tails > 0) || (heads == 0 && tails > 0)) {

              logger.info("Processing delta")
              locations.foreach(location => {
                val loc = location._1
                val ver = location._3

                var tile = location._2
                var dfTail = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail").drop("group")
                var dfHead = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head").drop("group")
                val newInsertsDF = dfTail.drop("ts").as("tail").join(dfHead.drop("ts").as("head"), cond, "leftanti")

                if (!columnTs.equals("None")) {
                  val newUpdatesDF = dfTail.as("tail").join(dfHead.as("head"), cond, "inner").filter($"tail.ts" > $"head.ts").selectExpr(pks.map(x => s"tail.$x"): _*)
                  if (newInsertsDF.count() > 0 || newUpdatesDF.count() > 0) {
                    //logger.info("Detected a change in C*")
                    val sourceDfV3 = newInsertsDF.drop("ts", "group").union(newUpdatesDF)
                    persistToTarget(sourceDfV3, columns, columnsPos, tile, ver)
                  }
                } else {
                  if (newInsertsDF.count() > 0) {
                    //logger.info("Detected a insert in C*")
                    val sourceDfV3 = newInsertsDF.drop("ts", "group")
                    persistToTarget(sourceDfV3, columns, columnsPos, tile, ver)
                  }
                }
                keyspacesConn.withSessionDo {
                  session => session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'$ver','SUCCESS', toTimestamp(now()), '')")
                }
              })
            }
          }
        }
      }
    }

    def keysDiscoveryProcess() {
      val primaryKeysDf = sparkSession.read.option("inferSchema", "true").table(source)
      val primaryKeysDfwithTS = primaryKeysDf.selectExpr(pkFinal.map(c => c): _*)
      val groupedPkDF = primaryKeysDfwithTS.withColumn("group", abs(xxhash64(pkFinalWithoutTs.map(c => col(c)): _*)) % totalTiles).repartition(col("group")).persist(StorageLevel.MEMORY_AND_DISK)
      val tiles = (0 to totalTiles - 1).toList.par
      tiles.foreach(tile => {
        keyspacesConn.withSessionDo {
          session => {
            var tailLoadStatus = ""
            var headLoadStatus = ""
            val rsTail = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='tail'").one()
            val rsHead = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='head'").one()

            val tail = Option(rsTail)
            val head = Option(rsHead)

            if (!tail.isEmpty) {
              tailLoadStatus = rsTail.getString("load_status")
            }
            if (!head.isEmpty) {
              headLoadStatus = rsHead.getString("load_status")
            }

            logger.info(s"Processing $tile, head is $head, tail is $tail, head status is $headLoadStatus, tail status is $tailLoadStatus")

            // Swap tail and head
            if ((!tail.isEmpty && tailLoadStatus == "SUCCESS") && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Swapping the tail and the head")

              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              val oldTail = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail").repartition(pks.map(c => col(c)): _*)

              oldTail.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")

              keyspacesConn.withSessionDo {
                session => {
                  session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
                  session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
                }
              }
            }

            // The second round (tail and head)
            if (tail.isEmpty && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Loading a tail but keeping the head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
              keyspacesConn.withSessionDo {
                session => session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
              }
            }

            // Historical upload, the first round (head)
            if (tail.isEmpty && head.isEmpty) {
              logger.info("Loading a head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              keyspacesConn.withSessionDo {
                session => session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
              }
              val cnt = staged.count()
              val content = s"""{"tile":$tile, "primaryKeys":$cnt}"""
              putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
            }
          }
        }
      })
      groupedPkDF.unpersist()
    }

    while (true) {
      if (processType == "discovery") {
        if (!stopRequested(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/$processType/stopRequested")) {
          keysDiscoveryProcess
        } else {
          logger.info("Stop was requested for the discovery process...")
          sys.exit()
        }
      }
      if (processType == "replication") {
        if (!stopRequested(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested")) {
          dataReplicationProcess
        } else {
          logger.info("Stop was requested for the replication process...")
          sys.exit()
        }
      }
      Thread.sleep(WAIT_TIME)
    }
    Job.commit()
  }
}