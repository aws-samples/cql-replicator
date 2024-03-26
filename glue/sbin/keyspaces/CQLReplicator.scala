/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon Keyspaces

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsV2Request}
import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy

import com.datastax.spark.connector.cql._
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.servererrors._
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids

import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.github.resilience4j.core.IntervalFunction

import scala.util.{Failure, Success, Try}
import scala.io.Source
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.util.matching.Regex

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.util.Base64
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.Duration
import java.nio.ByteBuffer
import org.joda.time.LocalDateTime
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4CompressorWithLength}

class LargeObjectException(s: String) extends RuntimeException {
  println(s)
}

class ProcessTypeException(s: String) extends RuntimeException {
  println(s)
}

class CassandraTypeException(s: String) extends RuntimeException {
  println(s)
}

class StatsS3Exception(s: String) extends RuntimeException {
  println(s)
}

class CompressionException(s: String) extends RuntimeException {
  println(s)
}

class CustomSerializationException(s: String) extends RuntimeException {
  println(s)
}

class DlqS3Exception(s: String) extends RuntimeException {
  println(s)
}

class CompressionException(s: String) extends RuntimeException {
  println(s)
}

sealed trait Stats

case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats

case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats

case class MaterialzedViewConfig(enabled: Boolean = false, mvName: String = "")

case class Replication(allColumns: Boolean = true, columns: List[String] = List(""), useCustomSerializer: Boolean = false, useMaterializedView: MaterialzedViewConfig = MaterialzedViewConfig())

case class CompressionConfig(enabled: Boolean = false, compressNonPrimaryColumns: List[String] = List(""), compressAllNonPrimaryColumns: Boolean = false, targetNameColumn: String = "")
case class LargeObjectsConfig(enabled: Boolean = false, column: String = "", bucket: String = "", prefix: String = "", enableRefByTimeUUID: Boolean = false, xref: String = "")
case class Keyspaces(compressionConfig: CompressionConfig, largeObjectsConfig: LargeObjectsConfig)

case class JsonMapping(replication: Replication, keyspaces: Keyspaces)

// **************************Custom JSON Serialzer Start*******************************************
class CustomResultSetSerializer extends org.json4s.Serializer[com.datastax.oss.driver.api.core.cql.Row] {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def binToHex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => {
        val output = bytes.map("%02x".format(_)).mkString
        s"0x$output"
      }
      case _ => {
        val output = bytes.map("%02x".format(_)).mkString(sep.get)
        s"0x$output"
      }
    }
  }

  override def serialize(implicit format: org.json4s.Formats): PartialFunction[Any, JValue] = {
    case row: com.datastax.oss.driver.api.core.cql.Row =>
      val names = row.getColumnDefinitions.asScala.map(_.getName.asCql(true)).toList
      val values = (0 until row.getColumnDefinitions.asScala.size).map { i =>
        val name = names(i)
        val dataType = row.getColumnDefinitions.get(i).getType
        if (row.isNull(i)) {
          name -> null
        } else {
          val value = getValue(row, i, dataType)
          name -> value
        }
      }.toMap
      Extraction.decompose(values)
  }

  private def getValue(row: com.datastax.oss.driver.api.core.cql.Row, i: Int, dataType: DataType): Any = {
    dataType match {
      case DataTypes.BOOLEAN => row.getBoolean(i)
      case DataTypes.INT => row.getInt(i)
      case DataTypes.TEXT => row.getString(i).replace("'", "\\\\u0027")
      case DataTypes.ASCII => row.getString(i).replace("'", "\\\\u0027")
      case DataTypes.BIGINT => row.getLong(i)
      case DataTypes.BLOB => binToHex(row.getByteBuffer(i).array())
      case DataTypes.COUNTER => row.getLong(i)
      case DataTypes.DATE => row.getLocalDate(i).format(DateTimeFormatter.ISO_LOCAL_DATE)
      case DataTypes.DECIMAL => row.getBigDecimal(i)
      case DataTypes.DOUBLE => row.getDouble(i)
      case DataTypes.FLOAT => row.getFloat(i)
      case DataTypes.TIMESTAMP => row.getInstant(i).toString.replace("T", " ")
      case DataTypes.SMALLINT => row.getInt(i)
      case DataTypes.TIMEUUID => row.getUuid(i).toString
      case DataTypes.UUID => row.getUuid(i).toString
      case DataTypes.TINYINT => row.getByte(i)
      case _ => throw new CustomSerializationException(s"Unsupported data type: $dataType")
    }
  }

  override def deserialize(implicit format: org.json4s.Formats): PartialFunction[(org.json4s.TypeInfo, JValue), com.datastax.oss.driver.api.core.cql.Row] = {
    ???
  }
}

// *******************************Custom JSON Serializer End*********************************************

class SupportFunctions() {
  def correctEmptyBinJsonValues(cols: List[String], input: String): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    if (cols.isEmpty) {
      input
    } else {
      val json = parse(input)

      @tailrec
      def replace(json: JValue, cols: Seq[String]): JValue = cols match {
        case Nil => json
        case col :: tail =>
          val updatedJson = (json \ col) match {
            case JString("") => json transformField {
              case JField(`col`, _) => JField(col, JString("0x"))
            }
            case _ => json
          }
          replace(updatedJson, tail)
      }

      val finalJson = replace(json, cols)
      compact(render(finalJson))
    }
  }
}

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r

    def convertCassandraKeyToGenericKey(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString(":")
    }

    def isLogObjectPresent(ksName: String,
                           tblName: String,
                           bucketName: String,
                           s3Client: com.amazonaws.services.s3.AmazonS3,
                           tile: Int, op: String): Option[String] = {
      val prefix = s"$ksName/$tblName/dlq/$tile/$op"
      val listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix)
      val objectListing = s3Client.listObjectsV2(listObjectsRequest)
      val firstObjectKeyOption = objectListing.getObjectSummaries.asScala.headOption.map(_.getKey)
      firstObjectKeyOption
    }

    // Strictly Serializable
    def replayLogs(logger: GlueLogger,
                   ksName: String,
                   tblName: String,
                   bucketName: String,
                   s3Client: com.amazonaws.services.s3.AmazonS3,
                   tile: Int,
                   op: String,
                   cc: CassandraConnector ): Unit = {
        val session = cc.openSession
        Iterator.continually(isLogObjectPresent(ksName, tblName, bucketName, s3Client, tile, op)).takeWhile(_.nonEmpty).foreach {
          key => {
            val keyTmp = key.get
            val getObjectRequest = new GetObjectRequest(bucketName, keyTmp)
            val s3Object = s3Client.getObject(getObjectRequest)
            val objectContent = scala.io.Source.fromInputStream(s3Object.getObjectContent).mkString
            Try {
              logger.info(s"Detected a failed $op '$keyTmp' in the dlq. The $op is going to be replayed.")
              session.execute(s"$objectContent IF NOT EXISTS").all()
            } match {
              case Failure(_) =>
              case Success(_) => {
                s3Client.deleteObject(bucketName, keyTmp)
                logger.info(s"Operation $op '$keyTmp' replayed and removed from the dlq successfully.")
              }
            }
        }
      }
        session.close()
    }

    def cleanupLedger(cc: CassandraConnector,
                      logger: GlueLogger,
                      ks: String, tbl: String,
                      cleanUpRequested: Boolean,
                      pt: String): Unit = {
      if (pt.equals("discovery") && cleanUpRequested) {
        cc.withSessionDo {
          session => {
            session.execute(s"DELETE FROM migration.ledger WHERE ks='$ks' and tbl='$tbl'")
          }
            logger.info("Cleaned up the migration.ledger")
            session.close()
        }
      }
    }

    def readReplicationStatsObject(s3Client: com.amazonaws.services.s3.AmazonS3, bucket: String, key: String): ReplicationStats = {
      Try {
        val s3Object = s3Client.getObject(bucket, key)
        val src = Source.fromInputStream(s3Object.getObjectContent)
        val json = src.getLines.mkString
        src.close()
        json
      } match {
        case Failure(_) => {
          ReplicationStats(0, 0, 0, 0, 0, LocalDateTime.now().toString)
        }
        case Success(json) => {
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(json).extract[ReplicationStats]
        }
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
        case "partitionKeys" =>
          meta.getPartitionKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase))
        case "primaryKeys" =>
          meta.getPrimaryKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase))
        case "primaryKeysWithTS" =>
          meta.getPrimaryKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase)) :+ Map(s"writetime($columnTs) as ts" -> "bigint")
        case _ =>
          meta.getPrimaryKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase))
      }
    }

    def getAllColumns(cc: CassandraConnector, ks: String, tbl: String): Seq[scala.collection.immutable.Map[String, String]] = {
      cc.withSessionDo {
        session =>
          session.getMetadata.getKeyspace(ks).get().
            getTable(tbl).get().
            getColumns.entrySet.asScala.
            map(x => Map(x.getKey.toString -> x.getValue.getType.toString)).toSeq
      }
    }

    /*
    def parseJSONMapping(s: String): JsonMapping = s match {
      case str if str.equals("None") => JsonMapping(Replication(), Keyspaces(CompressionConfig(), LargeObjectsConfig()))
      case str if !str.equals("None") => {
        implicit val formats: DefaultFormats.type = DefaultFormats
        parse(s).extract[JsonMapping]
      }
    }
     */

    def parseJSONMapping(s: String): JsonMapping = {
      Option(s) match {
        case Some("None") => JsonMapping(Replication(), Keyspaces(CompressionConfig(), LargeObjectsConfig()))
        case _ =>
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(s).extract[JsonMapping]
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
        case Failure(r) => {
          val err = s"ERROR: Detected connectivity issue. Check the reference conf file/Glue connection for the $dir, the job is aborted"
          logger.error(s"$err ${r.toString}")
          sys.exit(-1)
        }
        case Success(_) => {
          logger.info(s"Connected to the $dir")
        }
      }
    }

    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val sparkConf: SparkConf = sparkContext.getConf
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "TILE", "TOTAL_TILES", "PROCESS_TYPE", "SOURCE_KS", "SOURCE_TBL", "TARGET_KS", "TARGET_TBL", "WRITETIME_COLUMN", "TTL_COLUMN", "S3_LANDING_ZONE", "REPLICATION_POINT_IN_TIME", "SAFE_MODE", "CLEANUP_REQUESTED", "JSON_MAPPING", "REPLAY_LOG").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val jobRunId = args("JOB_RUN_ID")
    val currentTile = args("TILE").toInt
    val totalTiles = args("TOTAL_TILES").toInt
    val safeMode = args("SAFE_MODE")
    val replayLog = Try(args("REPLAY_LOG").toBoolean).getOrElse(false)
    // Internal configuration
    val WAIT_TIME = safeMode match {
      case "true" => 20000
      case _ => 0
    }
    val cachingMode = safeMode match {
      case "true" => StorageLevel.DISK_ONLY
      case _ => StorageLevel.MEMORY_AND_DISK_SER
    }
    // An increased number of retry attempts could potentially result in a significant number of operations being delayed
    val MAX_RETRY_ATTEMPTS = 256
    // Unit ms
    val EXP_BACKOFF = 25
    sparkSession.conf.set(s"spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")

    val processType = args("PROCESS_TYPE") // discovery or replication
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r

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
    val ttlColumn = args("TTL_COLUMN")
    val jsonMapping = args("JSON_MAPPING")
    val replicationPointInTime = args("REPLICATION_POINT_IN_TIME").toLong
    val defaultPartitions = scala.math.max(2, (sparkContext.defaultParallelism / 2 - 2))
    val cleanUpRequested: Boolean = args("CLEANUP_REQUESTED") match {
      case "false" => false
      case _ => true
    }

    //AmazonS3Client to check if a stop request was issued
    val s3ClientConf = new ClientConfiguration().withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    val s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(s3ClientConf).build()

    // Let's do preflight checks
    logger.info("Preflight check started")
    preFlightCheck(cassandraConn, srcKeyspaceName, srcTableName, "source")
    preFlightCheck(keyspacesConn, trgKeyspaceName, trgTableName, "target")
    logger.info("Preflight check completed")

    val pkFinal = columnTs match {
      case "None" => inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
      case _ => inferKeys(cassandraConn, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    }

    val pkFinalWithoutTs = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val pks = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex

    val allColumnsFromSource = getAllColumns(cassandraConn, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList

    val jsonMappingRaw = new String(Base64.getDecoder.decode(jsonMapping.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    logger.info(s"Json mapping: $jsonMappingRaw")

    val jsonMapping4s = parseJSONMapping(jsonMappingRaw.replaceAll("\\r\\n|\\r|\\n", ""))
    val replicatedColumns = jsonMapping4s match {
      case JsonMapping(Replication(true, _, _, _), _) => "*"
      case rep => rep.replication.columns.mkString(",")
    }

    val selectStmtWithTTL = ttlColumn match {
      case s if s.equals("None") => ""
      case s if (!s.equals("None") && replicatedColumns.equals("*")) => {
        val lstColumns = allColumnsFromSource.flatMap(_.keys).toSeq :+ s"ttl($ttlColumn)"
        lstColumns.mkString(",")
      }
      case s if !(s.equals("None") || replicatedColumns.equals("*")) => {
        val lstColumns = s"$replicatedColumns,ttl($ttlColumn)"
        lstColumns
      }
    }

    def binToHex(bytes: Array[Byte], sep: Option[String] = None): String = {
      sep match {
        case None => {
          val output = bytes.map("%02x".format(_)).mkString
          s"0x$output"
        }
        case _ => {
          val output = bytes.map("%02x".format(_)).mkString(sep.get)
          s"0x$output"
        }
      }
    }

    def compressWithLZ4B(input: String): Array[Byte] = {
      Try {
        val lz4Factory = LZ4Factory.fastestInstance()
        val compressor: LZ4Compressor = lz4Factory.fastCompressor()
        val inputBytes = input.getBytes("UTF-8")
        val compressorWithLength = new LZ4CompressorWithLength(compressor)
        compressorWithLength.compress(inputBytes)
      } match {
        case Failure(e) => throw new CompressionException(s"Unable to compress due to $e")
        case Success(res) => res
      }
    }

    def stopRequested(bucket: String): Boolean = {
      val key = processType match {
        case "discovery" => s"$srcKeyspaceName/$srcTableName/$processType/stopRequested"
        case "replication" => s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested"
        case _ => throw new ProcessTypeException("Unrecognizable process type")
      }
      Try {
        s3client.getObject(bucket, key)
        s3client.deleteObject(bucket, key)
        logger.info(s"Requested a stop for $key process")
      } match {
        case Failure(_) => false
        case Success(_) => true
      }
    }

    def removeLeadingEndingSlashes(str: String): String = {
      str.
        stripPrefix("/").
        stripSuffix("/")
    }

    def removeS3prefixIfPresent(str: String): String = {
      str.stripPrefix("s3://")
    }

    def offloadToS3(jPayload: org.json4s.JValue, s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3, whereClause: String): JValue = {
      val localConfig = jsonMapping4s.keyspaces.largeObjectsConfig
      val columnName = localConfig.column
      val bucket = removeS3prefixIfPresent(localConfig.bucket)
      val prefix = removeLeadingEndingSlashes(localConfig.prefix)
      val xrefColumnName = localConfig.xref
      val key = Uuids.timeBased().toString
      val largeObject = Base64.getEncoder.encodeToString(compressWithLZ4B((jPayload \ columnName).values.toString))

      val updatedJsonStatement = localConfig.enableRefByTimeUUID match {
        case true => {
          val jsonStatement = jPayload transformField {
            case JField(`xrefColumnName`, _) => JField(xrefColumnName, org.json4s.JsonAST.JString(s"$key"))
          }
          jsonStatement removeField {
            case JField(`columnName`, _) => true
            case _ => false
          }
        }
        case _ => {
          jPayload removeField {
            case JField(`columnName`, _) => true
            case _ => false
          }
        }
      }

      val targetPrefix = localConfig.enableRefByTimeUUID match {
        case true => {
          s"$prefix/$key"
        }
        case _ => {
          // Structure: key=pk1:pk2..cc1:cc2/payload
          s"$prefix/key=${convertCassandraKeyToGenericKey(whereClause)}/payload"
        }
      }

      Try {
        s3ClientOnPartition.putObject(bucket, targetPrefix, largeObject)
      } match {
        case Failure(r) => throw new LargeObjectException(s"Not able to persist the large object to the S3 bucket due to $r")
        case Success(_) => updatedJsonStatement
      }
    }

    def compressValues(json: String): String = {
      if (jsonMapping4s.keyspaces.compressionConfig.enabled) {
        val jPayload = parse(json)

        val compressColumns = jsonMapping4s.keyspaces.compressionConfig.compressAllNonPrimaryColumns match {
          case true => {
            val acfs = allColumnsFromSource.flatMap(_.keys).toSet
            val excludePks = columns.map(x => x._1.toString).toSet
            acfs.filterNot(excludePks.contains(_))
          }
          case _ => jsonMapping4s.keyspaces.compressionConfig.compressNonPrimaryColumns.toSet
        }
        val filteredJson = jPayload.removeField {
          case (key, _) =>
            compressColumns.contains(key)
        }
        val excludedJson = jPayload.filterField {
          case (key, _) =>
            compressColumns.contains(key)
        }
        val updatedJson = excludedJson.isEmpty match {
          case false => {
            val compressedPayload: Array[Byte] = compressWithLZ4B(compact(render(JObject(excludedJson))))
            filteredJson merge JObject(jsonMapping4s.keyspaces.compressionConfig.targetNameColumn -> JString(binToHex(compressedPayload)))
          }
          case _ => throw new CompressionException("Compressed payload is empty")
        }
        compact(render(updatedJson))
      } else {
        json
      }
    }

    def persistToDlq(s3Client: com.amazonaws.services.s3.AmazonS3, bucket: String, key: String, event: String): Unit = {
      val ts = LocalDateTime.now().toString
      s3Client.putObject(bucket, s"$key/log-$ts.msg", event)
    }

    def putStats(bucket: String, key: String, objectName: String, stats: Stats): Unit = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val (newContent, message) = stats match {
        case ds: DiscoveryStats =>
          (write(ds), s"Flushing the discovery stats: $key/$objectName")
        case rs: ReplicationStats =>
          val content = readReplicationStatsObject(s3client, bucket, s"$key/$objectName")
          val insertedAggr = content.insertedPrimaryKeys + rs.insertedPrimaryKeys
          val updatedAggr = content.updatedPrimaryKeys + rs.updatedPrimaryKeys
          val deletedAggr = content.deletedPrimaryKeys + rs.deletedPrimaryKeys
          val historicallyInserted = content.primaryKeys + rs.primaryKeys
          (write(ReplicationStats(currentTile,
            historicallyInserted,
            updatedAggr,
            insertedAggr,
            deletedAggr,
            LocalDateTime.now().toString)),
            s"Flushing the replication stats: $key/$objectName")
        case _ => throw new StatsS3Exception("Unknown stats type")
      }
      Try {
        s3client.putObject(bucket, s"$key/$objectName", newContent)
      } match {
        case Failure(_) => throw new StatsS3Exception(s"Can't persist the stats to the S3 bucket $bucket")
        case Success(_) => logger.info(message)
      }
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

    def getSourceRow(cls: String, wc: String, session: CqlSession, defaultFormat: org.json4s.Formats): String = {
      val rs = jsonMapping4s.replication.useCustomSerializer match {
        case false => {
          val row = Option(session.execute(s"SELECT json $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc").one())
          row.get.getString(0).replace("'", "\\\\u0027")
        }
        case _ => {
          val row = Option(session.execute(s"SELECT $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc").one())
          Serialization.write(row)(defaultFormat)
        }
      }
      rs
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      df.rdd.foreachPartition(
        partition => {
          val customFormat = jsonMapping4s.replication.useCustomSerializer match {
            case true => DefaultFormats + new CustomResultSetSerializer
            case _ => DefaultFormats
          }
          val supportFunctions = new SupportFunctions()
          val retryConfig = RetryConfig.custom.maxAttempts(MAX_RETRY_ATTEMPTS).
            intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(EXP_BACKOFF), 1.1)).
            retryExceptions(classOf[WriteFailureException], classOf[WriteTimeoutException], classOf[ServerError], classOf[UnavailableException], classOf[NoNodeAvailableException], classOf[AllNodesFailedException]).build()
          val retry = Retry.of("keyspaces", retryConfig)
          val s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3 = AmazonS3ClientBuilder.defaultClient()
          /* Only for debugging
          val publisher = retry.getEventPublisher
          publisher.onIgnoredError(event => {
            logger.info(s"Operation was failed on event $event. The event should be reprocessed")
          })
          */

          partition.foreach(
            row => {
              val whereClause = rowToStatement(row, columns, columnsPos)
              if (whereClause.nonEmpty) {
                if (op == "insert" || op == "update") {
                  cassandraConn.withSessionDo { session => {
                    if (ttlColumn.equals("None")) {
                      val rs = getSourceRow(replicatedColumns, whereClause, session, customFormat)
                      if (rs.nonEmpty) {
                        val jsonRowEscaped = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs)
                        val jsonRow = compressValues(jsonRowEscaped)
                        keyspacesConn.withSessionDo {
                          session => {
                            jsonMapping4s.keyspaces.largeObjectsConfig.enabled match {
                              case false => {
                                val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$jsonRow'"
                                val resTry = Try(Retry.decorateSupplier(retry, () => session.execute(cqlStatement)).get())
                                resTry match {
                                  case Success(_) =>
                                  case Failure(_) => persistToDlq(s3ClientOnPartition, bcktName, s"$srcKeyspaceName/$srcTableName/dlq/$tile/$op", cqlStatement)
                                }
                              }
                              case _ => {
                                val json4sRow = parse(jsonRow)
                                val updatedJsonRow = compact(render(offloadToS3(json4sRow, s3ClientOnPartition, whereClause)))
                                val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$updatedJsonRow'"
                                val resTry = Try(Retry.decorateSupplier(retry, () => session.execute(cqlStatement)).get())
                                resTry match {
                                  case Success(_) =>
                                  case Failure(_) => persistToDlq(s3ClientOnPartition, bcktName, s"$srcKeyspaceName/$srcTableName/dlq/$tile/$op", cqlStatement)
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    else {
                      val rs = getSourceRow(selectStmtWithTTL, whereClause, session, customFormat)
                      if (rs.nonEmpty) {
                        val jsonRowEscaped = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs)
                        val jsonRow = compressValues(jsonRowEscaped)
                        val json4sRow = parse(jsonRow)
                        keyspacesConn.withSessionDo {
                          session => {
                            jsonMapping4s.keyspaces.largeObjectsConfig.enabled match {
                              case false => {
                                val backToJsonRow = backToCQLStatementWithoutTTL(json4sRow)
                                val ttlVal = getTTLvalue(json4sRow)
                                Retry.decorateSupplier(retry, () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal")).get()
                              }
                              case _ => {
                                val json4sRow = parse(jsonRow)
                                val updatedJsonRow = offloadToS3(json4sRow, s3ClientOnPartition, whereClause)
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
                if (op == "delete") {
                  keyspacesConn.withSessionDo {
                    session => {
                      val cqlStatement = s"DELETE FROM $trgKeyspaceName.$trgTableName WHERE $whereClause"
                      val resTry = Try(Retry.decorateSupplier(retry, () => session.execute(cqlStatement)).get())
                      resTry match {
                        case Success(_) =>
                        case Failure(_) => persistToDlq(s3ClientOnPartition, bcktName, s"$srcKeyspaceName/$srcTableName/dlq/$tile/$op", cqlStatement)
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

    def listWithSingleQuotes(lst: java.util.List[String], colType: String): String = {
      colType match {
        case patternForSingleQuotes(_*) => lst.asScala.toList.map(c => s"'$c'").mkString("[", ",", "]")
        case _ => lst.asScala.toList.map(c => s"$c").mkString("[", ",", "]")
      }
    }

    def rowToStatement(row: org.apache.spark.sql.Row, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)]): String = {
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

    def dataReplicationProcess(): Unit = {
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
                val sourcePath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/$loc"
                val sourceDf = glueContext.getSourceWithFormat(
                  connectionType = "s3",
                  format = "parquet",
                  options = JsonOptions(s"""{"paths": ["$sourcePath"]}""")
                ).getDynamicFrame().toDF()

                val sourceDfV2 = sourceDf.drop("group").drop("ts")
                val tile = location._2

                persistToTarget(shuffleDfV2(sourceDfV2), columns, columnsPos, tile, "insert")
                session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'head','SUCCESS', toTimestamp(now()), '')")
                val cnt = sourceDfV2.count()

                val content = ReplicationStats(tile, cnt, 0, 0, 0, LocalDateTime.now().toString)
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)

              }
              )
            }

            if ((heads > 0 && tails > 0) || (heads == 0 && tails > 0)) {
              var inserted: Long = 0
              var deleted: Long = 0
              var updated: Long = 0

              logger.info("Processing delta...")
              val pathTail = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.tail"
              val dfTail = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$pathTail"]}""")
              ).getDynamicFrame().toDF().drop("group").persist(cachingMode)

              val pathHead = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.head"
              val dfHead = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$pathHead"]}""")
              ).getDynamicFrame().toDF().drop("group").persist(cachingMode)

              val newInsertsDF = dfTail.drop("ts").as("tail").join(dfHead.drop("ts").as("head"), cond, "leftanti").persist(cachingMode)
              val newDeletesDF = dfHead.drop("ts").as("head").join(dfTail.drop("ts").as("tail"), cond, "leftanti").persist(cachingMode)

              columnTs match {
                case "None" => {
                  if (!newInsertsDF.isEmpty) {
                    persistToTarget(newInsertsDF, columns, columnsPos, currentTile, "insert")
                    inserted = newInsertsDF.count()
                  }
                }
                case _ => {
                  val newUpdatesDF = dfTail.as("tail").join(dfHead.as("head"), cond, "inner").
                    filter($"tail.ts" > $"head.ts").
                    selectExpr(pks.map(x => s"tail.$x"): _*).persist(cachingMode)
                  if (!(newInsertsDF.isEmpty && newUpdatesDF.isEmpty)) {
                    persistToTarget(newInsertsDF, columns, columnsPos, currentTile, "insert")
                    persistToTarget(newUpdatesDF, columns, columnsPos, currentTile, "update")
                    inserted = newInsertsDF.count()
                    updated = newUpdatesDF.count()
                  }
                  newUpdatesDF.unpersist()
                }
              }

              if (!newDeletesDF.isEmpty) {
                persistToTarget(newDeletesDF, columns, columnsPos, currentTile, "delete")
                deleted = newDeletesDF.count()
              }

              if (!(updated != 0 && inserted != 0 && deleted != 0)) {
                val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, LocalDateTime.now().toString)
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
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

    def keysDiscoveryProcess(): Unit = {
      val srcTableForDiscovery = if (jsonMapping4s.replication.useMaterializedView.enabled) {
        jsonMapping4s.replication.useMaterializedView.mvName
      } else {
        srcTableName
      }
      val primaryKeysDf = columnTs match {
        case "None" =>
          sparkSession.read.cassandraFormat(srcTableForDiscovery, srcKeyspaceName).option("inferSchema", "true").
            load().
            selectExpr(pkFinal.map(c => c): _*).
            withColumn("ts", lit(0)).
            persist(cachingMode)
        case ts if ts != "None" && replicationPointInTime == 0 =>
          sparkSession.read.cassandraFormat(srcTableForDiscovery, srcKeyspaceName).option("inferSchema", "true").
            load().
            selectExpr(pkFinal.map(c => c): _*).
            persist(cachingMode)
        case ts if ts != "None" && replicationPointInTime > 0 =>
          sparkSession.read.cassandraFormat(srcTableForDiscovery, srcKeyspaceName).option("inferSchema", "true").
            load().
            selectExpr(pkFinal.map(c => c): _*).
            filter(($"ts" > replicationPointInTime) && ($"ts".isNotNull)).
            persist(cachingMode)
      }

      val groupedPkDF = primaryKeysDf.withColumn("group", abs(xxhash64(pkFinalWithoutTs.map(c => col(c)): _*)) % totalTiles).
        repartition(col("group")).persist(cachingMode)
      val tiles = (0 until totalTiles).toList.par
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

              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              val oldTailPath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail"
              val oldTail = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$oldTailPath"]}""")
              ).getDynamicFrame().toDF().repartition(defaultPartitions, pks.map(c => col(c)): _*)

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
              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
            }

            // Historical upload, the first round (head)
            if (tail.isEmpty && head.isEmpty) {
              logger.info("Loading a head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
              val content = DiscoveryStats(tile, staged.count(), LocalDateTime.now().toString)
              putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
            }
          }
            session.close()
        }
      })
      groupedPkDF.unpersist()
      primaryKeysDf.unpersist()
    }

    cleanupLedger(keyspacesConn, logger, srcKeyspaceName, srcTableName, cleanUpRequested, processType)

    Iterator.continually(stopRequested(bcktName)).takeWhile(_ == false).foreach {
      _ => {
        processType match {
          case "discovery" => {
            keysDiscoveryProcess
          }
          case "replication" => {
            if (replayLog) {
              // Replay inserts
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "insert", keyspacesConn)
              // Replay updates
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "update", keyspacesConn)
              // Replay deletes
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "delete", keyspacesConn)
            }
            dataReplicationProcess
          }
          case _ => {
            logger.info(s"Unrecognizable process type - $processType")
            sys.exit()
          }
        }
        logger.info(s"Cool down period $WAIT_TIME ms")
        Thread.sleep(WAIT_TIME)
      }
    }
    logger.info(s"Stop was requested for the $processType process...")
    Job.commit()
  }
}