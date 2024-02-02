/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

// Target Amazon MemoryDB

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
import java.util.Optional
import org.joda.time.LocalDateTime

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ObjectMetadata, DeleteObjectsRequest, ObjectListing, S3ObjectSummary}
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector._
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.servererrors._

import scala.util.{Try, Success, Failure}
import scala.util.matching.Regex
import scala.io.Source
import scala.collection.mutable.StringBuilder

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Base64
import java.nio.charset.StandardCharsets

import redis.clients.jedis.Connection
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisCluster, HostAndPort}

import net.jpountz.xxhash.XXHashFactory

class LargeObjectException(s: String) extends RuntimeException {
  println(s)
}

class ProcessTypeException(s: String) extends RuntimeException {
  println(s)
}

class CassandraTypeException(s: String) extends RuntimeException {
  println(s)
}

class RedisConnectionException(s: String) extends RuntimeException {
  println(s)
}

case class RedisConfig(pwd: String, usr: String, clusterDnsName: String, clusterPort: Int, sslEnabled: Boolean, maxAttempts: Int, useXXHash64Key: Boolean, connectionTimeout: Int, soTimeout: Int, xxHash64Seed: Long)

object GlueApp {
  def main(sysArgs: Array[String]) {

    def readRedisConfigFile(s3Client: com.amazonaws.services.s3.AmazonS3, bucket: String, key: String): RedisConfig = {
      val s3Object = s3Client.getObject(bucket, key)
      val src = Source.fromInputStream(s3Object.getObjectContent())
      val json = src.getLines.mkString
      src.close()

      implicit val formats = DefaultFormats
      parse(json).extract[RedisConfig]
    }

    def getRedisConnection(redisConfig: RedisConfig, clientName: String): JedisCluster = {
      val poolConfig = new GenericObjectPoolConfig[Connection]()
      redisConfig.usr match {
        case usr if usr.isEmpty => new JedisCluster(new HostAndPort(redisConfig.clusterDnsName, redisConfig.clusterPort),
          redisConfig.connectionTimeout,
          redisConfig.soTimeout,
          redisConfig.maxAttempts,
          null,
          null,
          poolConfig,
          redisConfig.sslEnabled)
        case _ => new JedisCluster(new HostAndPort(redisConfig.clusterDnsName, redisConfig.clusterPort),
          redisConfig.connectionTimeout,
          redisConfig.soTimeout,
          redisConfig.maxAttempts,
          redisConfig.usr,
          redisConfig.pwd,
          clientName,
          poolConfig,
          redisConfig.sslEnabled)
      }
    }

    def shuffleDf(df: DataFrame): DataFrame = {
      val encoder = RowEncoder(df.schema)
      df.mapPartitions(new scala.util.Random().shuffle(_))(encoder)
    }

    def shuffleDfV2(df: DataFrame): DataFrame = {
      df.orderBy(rand())
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

    def preFlightCheck(connection: CassandraConnector, keyspace: String, table: String, dir: String): Unit = {
      val logger = new GlueLogger
      Try {
        val c1 = Option(connection.openSession)
        c1.isEmpty match {
          case false => {
            c1.get.getMetadata.getKeyspace(keyspace).isPresent match {
              case true => {
                c1.get.getMetadata.getKeyspace(keyspace).get.getTable(table).isPresent match {
                  case true => {
                    logger.info(s"the $dir table $table exists")
                  }
                  case false => {
                    val err = s"ERROR: the $dir table $table does not exist"
                    logger.error(err)
                    sys.exit(-1)
                  }
                }
              }
              case false => {
                val err = s"ERROR: the $dir keyspace $keyspace does not exist"
                logger.error(err)
                sys.exit(-1)
              }
            }
          }
          case _ => {
            val err = s"ERROR: The job was not able to connecto to the $dir"
            logger.error(err)
            sys.exit(-1)
          }
        }
      } match {
        case Failure(_) => {
          val err = s"ERROR: Detected connectivity issue. Check the reference conf file/Glue connection for the $dir, the job is aborted"
          logger.error(err)
          sys.exit(-1)
        }
        case Success(_) => {
          logger.info(s"Connected to the $dir")
        }
      }
    }

    val WAIT_TIME = 60000
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
    // Internal configuration+
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog.spark.cassandra.connection.config.profile.path", "KeyspacesConnector.conf")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster.spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")

    val ledgerTable = "ledger"
    val ledgerKeyspaces = "migration"
    val processType = args("PROCESS_TYPE") // discovery or replication
    val interanlLedger = s"ledgerCatalog.$ledgerKeyspaces.$ledgerTable"
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r

    // Business configuration
    val srcTableName = args("SOURCE_TBL")
    val srcKeyspaceName = args("SOURCE_KS")
    val trgTableName = args("TARGET_TBL")
    val trgKeyspaceName = args("TARGET_KS")
    val customConnections = customConnectionFactory(sparkContext)
    val cassandraConn = customConnections._2
    val keyspacesConn = customConnections._1
    val landingZone = args("S3_LANDING_ZONE")
    val bcktName = landingZone.replaceAll("s3://", "")
    val columnTs = args("WRITETIME_COLUMN")
    val source = s"sourceCluster.$srcKeyspaceName.$srcTableName"
    val ttlColumn = args("TTL_COLUMN")
    val olo = args("OFFLOAD_LARGE_OBJECTS")

    //AmazonS3Client to check if a stop requested issued
    val s3client = new AmazonS3Client()
    val redisConfig = readRedisConfigFile(s3client, bcktName, "artifacts/RedisConnector.conf")
    preFlightCheck(cassandraConn, srcKeyspaceName, srcTableName, "source")
    logger.info("[Cassandra] Preflight check is completed")
    Try {
      val preFlightCheckRedisConn = getRedisConnection(redisConfig, "Preflight check Redis connection")
      preFlightCheckRedisConn.close()
    } match {
      case Failure(_) => throw new RedisConnectionException("[Redis] Connection issue, please check the RedisConnector.conf and the Glue connector")
      case Success(_) => logger.info("[Redis] Preflight check is completed")
    }

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

    def convertToMap(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"'${m.group(1)}':'${m.group(2)}'" }.mkString(", ")
    }

    def convertToKeyValuePair(input: String, rawJson: org.json4s.JValue): (String, String) = {
      val key = patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString("#")
      val keysToRemove = columns.keys.toSet
      val updatedJson = rawJson.removeField {
        case JField(name, _) if keysToRemove.contains(name) => true
        case _ => false
      }
      val value = compact(render(updatedJson))
      (key, value)
    }

    def convertCassandraKeyToGenericKey(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString("#")
    }

    def stopRequested(bucket: String): Boolean = {
      val key = processType match {
        case "discovery" => s"$srcKeyspaceName/$srcTableName/$processType/stopRequested"
        case "replication" => s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested"
        case _ => throw new ProcessTypeException("Unrecognizable process type")
      }
      Try {
        val s3Object: S3Object = s3client.getObject(bucket, key)
        s3client.deleteObject(bucket, key)
        logger.info(s"Requested a stop for $key process")
      } match {
        case Failure(_) => false
        case Success(_) => true
      }
    }

    def cleanUpJsonObjects(bucket: String, key: String): Unit = {
      def deleteObjects(objectListing: ObjectListing): Unit = {
        val summaries = objectListing.getObjectSummaries.asScala.toList

        if (!summaries.isEmpty) {
          val deleteRequest = new DeleteObjectsRequest(bucket)
          deleteRequest.withKeys(summaries.map(_.getKey): _*)
          s3client.deleteObjects(deleteRequest)
        }
        if (objectListing.isTruncated) {
          deleteObjects(s3client.listNextBatchOfObjects(objectListing))
        }
      }

      deleteObjects(s3client.listObjects(bucket, key))
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

    def getJsonWithoutTTLColumn(jvalue: org.json4s.JValue): String = {
      val res = jvalue filterField (p => (p._1 != s"ttl($ttlColumn)"))
      val jsonNew = JObject(res)
      compact(render(jsonNew))
    }

    lazy val computeHash = (key: String, factory: net.jpountz.xxhash.StreamingXXHash64) => {
      redisConfig.useXXHash64Key match {
        case true => {
          val data = key.getBytes("UTF-8")
          factory.reset()
          factory.update(data, 0, data.length)
          factory.getValue.toString
        }
        case _ => key
      }
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String],
                        columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      df.rdd.foreachPartition(
        partition => {
          lazy val xxHashFactory = XXHashFactory.fastestInstance()
          lazy val xxHash64Seed = xxHashFactory.newStreamingHash64(redisConfig.xxHash64Seed)
          lazy val redisCluster = getRedisConnection(redisConfig, s"CQLReplicator$currentTile")
          partition.foreach(
            row => {
              val whereClause = rowToStatement(row, columns, columnsPos)
              if (!whereClause.isEmpty) {
                cassandraConn.withSessionDo { session => {
                  if (op == "insert" || op == "update") {
                    val rs = ttlColumn match {
                      case "None" => Option(session.execute(s"SELECT json * FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                      case _ => Option(session.execute(s"SELECT json $selectStmtWithTTL FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                    }
                    if (!rs.isEmpty) {
                      val jsonRow = rs.get.getString(0).replace("'", "\\\\u0027")
                      val res = convertToKeyValuePair(whereClause, parse(jsonRow))
                      val key = computeHash(res._1, xxHash64Seed)
                      if (ttlColumn.equals("None")) {
                        redisCluster.set(key, res._2)
                      } else {
                        val json4sRow = parse(res._2)
                        val jsonValueWithoutTTL = getJsonWithoutTTLColumn(json4sRow)
                        val ttl = getTTLvalue(json4sRow)
                        redisCluster.set(key, jsonValueWithoutTTL)
                        redisCluster.expire(key, ttl.toLong)
                      }
                    }
                  }
                  if (op == "delete") {
                    redisCluster.del(computeHash(convertCassandraKeyToGenericKey(whereClause), xxHash64Seed))
                  }
                }
                }
              }
            }
          )
          redisCluster.close()
          xxHash64Seed.close()
        }
      )
    }

    def listWithSingleQuotes(lst: java.util.List[String], colType: String): String = {
      colType match {
        case patternForSingleQuotes(_*) => lst.asScala.toList.map(c => s"'$c'").mkString("[", ",", "]")
        case _ => lst.asScala.toList.map(c => s"$c").mkString("[", ",", "]")
      }
    }

    def rowToStatement(row: Row, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)]): String = {
      val whereStmt = new StringBuilder
      columnsPos.foreach { el =>
        val colName = el._1
        val position = row.fieldIndex(el._1)
        val colType: String = columns.getOrElse(el._1, "none")
        val v = colType match {
          // inet is string
          case "string" | "text" | "inet" => s"'${row.getString(position)}'"
          case "date" => s"'${row.getDate(position)}'"
          case "timestamp" => s"'${row.getTimestamp(position)}'"
          case "int" => row.getInt(position)
          case "long" | "bigint" => row.getLong(position)
          case "float" => row.getFloat(position)
          case "double" => row.getDouble(position)
          case "short" => row.getShort(position)
          case "decimal" => row.getDecimal(position)
          case "tinyint" => row.getByte(position)
          case "uuid" => row.getString(position)
          case "boolean" => row.getBoolean(position)
          case "blob" => s"0${lit(row.getAs[Array[Byte]](colName)).toString.toLowerCase.replaceAll("'", "")}"
          case colType if colType.startsWith("list") => listWithSingleQuotes(row.getList[String](position), colType)
          case _ => throw new CassandraTypeException(s"Unrecognized data type $colType")
        }
        whereStmt.append(s"$colName=$v")
        el._2 match {
          case i if i < columns.size - 1 => whereStmt.append(" and ")
          case _ =>
        }
      }
      whereStmt.toString
    }

    def persistToRedis(df: DataFrame, op: String, tile: Int): Unit = {
      // Try !df.rdd.isEmpty
      // Try df.take(1).isEmpty
      if (!df.isEmpty) {
        persistToTarget(shuffleDfV2(df.drop("ts", "group")), columns, columnsPos, tile, op)
      }
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
                val tile = location._2
                val numPartitions = sourceDf.rdd.getNumPartitions
                logger.info(s"Number of partitions $numPartitions")
                persistToRedis(sourceDf, "insert", tile)
                session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'head','SUCCESS', toTimestamp(now()), '')")
                val cnt = sourceDf.count()
                val content = s"""{"tile":$tile, "primaryKeys":$cnt}"""
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)
              }
              )
            }
            // [Optimize] to ((tails > 0) && (heads > 0 || heads == 0))
            if ((heads > 0 && tails > 0) || (heads == 0 && tails > 0)) {
              logger.info("Processing delta...")
              val dfTail = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.tail").persist(StorageLevel.MEMORY_AND_DISK_SER)
              val dfHead = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.head").persist(StorageLevel.MEMORY_AND_DISK_SER)
              val newInsertsDF = dfTail.as("tail").join(dfHead.as("head"), cond, "leftanti").persist(StorageLevel.MEMORY_AND_DISK_SER)
              val newDeletesDF = dfHead.as("head").join(dfTail.as("tail"), cond, "leftanti").persist(StorageLevel.MEMORY_AND_DISK_SER)
              columnTs match {
                case "None" => {
                  persistToRedis(newInsertsDF, "insert", currentTile)
                  persistToRedis(newDeletesDF, "delete", currentTile)
                }
                case _ => {
                  val newUpdatesDF = dfTail.as("tail").
                    // Broadcast join
                    join(broadcast(dfHead.as("head")), cond, "inner").
                    filter($"tail.ts" > $"head.ts").
                    // [Optimize] selectExpr to select
                    selectExpr(pks.map(x => s"tail.$x"): _*).
                    persist(StorageLevel.MEMORY_AND_DISK_SER)
                  persistToRedis(newInsertsDF, "insert", currentTile)
                  persistToRedis(newUpdatesDF, "update", currentTile)
                  persistToRedis(newDeletesDF, "delete", currentTile)
                  newUpdatesDF.unpersist()
                }
              }
              newInsertsDF.unpersist()
              newDeletesDF.unpersist()
              dfTail.unpersist()
              dfHead.unpersist()
              session.execute(s"BEGIN UNLOGGED BATCH " +
                s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'tail','SUCCESS', toTimestamp(now()), '');" +
                s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'head','SUCCESS', toTimestamp(now()), '');" +
                s"APPLY BATCH;")
            }
          }
        }
          session.close()
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
            val rsTail = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='tail'").one()
            val rsHead = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='head'").one()

            val tail = Option(rsTail)
            val head = Option(rsHead)
            val tailLoadStatus = tail match {
              case t if !t.isEmpty => rsTail.getString("load_status")
              case _ => ""
            }
            val headLoadStatus = head match {
              case h if !h.isEmpty => rsHead.getString("load_status")
              case _ => ""
            }

            logger.info(s"Processing $tile, head is $head, tail is $tail, head status is $headLoadStatus, tail status is $tailLoadStatus")

            // Swap tail and head
            if ((!tail.isEmpty && tailLoadStatus == "SUCCESS") && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Swapping the tail and the head")

              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              val oldTail = sparkSession.read.option("inferSchema", "true").parquet(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail").repartition(pks.map(c => col(c)): _*)

              oldTail.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")

              session.execute(
                s"BEGIN UNLOGGED BATCH " +
                  s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','');" +
                  s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','');" +
                  s"APPLY BATCH;"
              )
            }

            // The second round (tail and head)
            if (tail.isEmpty && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Loading a tail but keeping the head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")

            }

            // Historical upload, the first round (head)
            if (tail.isEmpty && head.isEmpty) {
              logger.info("Loading a head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
              val cnt = staged.count()
              val content = s"""{"tile":$tile, "primaryKeys":$cnt}"""
              putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
            }
          }
            session.close()
        }
      })
      groupedPkDF.unpersist()
    }

    Iterator.continually(stopRequested(bcktName)).takeWhile(_ == false).foreach {
      _ => {
        processType match {
          case "discovery" => {
            keysDiscoveryProcess
          }
          case "replication" => {
            dataReplicationProcess
          }
          case _ => {
            logger.info(s"Unrecognizable process type - $processType")
            sys.exit()
          }
        }
        // [Optimize] remove it
        Thread.sleep(WAIT_TIME)
      }
    }
    logger.info(s"Stop was requested for the $processType process...")
    Job.commit()
  }
}