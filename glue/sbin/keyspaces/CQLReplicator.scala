/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon Keyspaces - Glue 5.0 Version

import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsV2Request}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.{AmazonServiceException, ClientConfiguration, SdkClientException}
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchStatementBuilder, DefaultBatchType, SimpleStatement}
import com.datastax.oss.driver.api.core.servererrors._
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.oss.driver.api.core.{AllNodesFailedException, CqlSession, DriverException, NoNodeAvailableException}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.{Retry, RetryConfig}
import net.jpountz.lz4.{LZ4Compressor, LZ4CompressorWithLength, LZ4Factory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.Base64
import scala.annotation.tailrec
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

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

class PreFlightCheckException(val message: String, val errorCode: Int, val cause: Throwable = null)
  extends Exception(s"[$errorCode] $message", cause) {
  def this(message: String) = this(message, 0)

  def this(message: String, cause: Throwable) = this(message, 0, cause)

  override def toString: String = s"PreFlightCheckException($errorCode, $message)"
}

sealed trait Stats

case class WriteConfiguration(batchSize: Int = 1024 * 1024, maxRetryAttempts: Int = 64, expBackoff: Int = 25, maxStatementsPerBatch: Int = 29, preShuffleBeforeWrite: Boolean = true)

case class ReadConfiguration(splitSizeInMB: Int = 64, concurrentReads: Int = 32, consistencyLevel: String = "LOCAL_ONE", fetchSizeInRows: Int = 500, queryRetryCount: Int = 180, readTimeoutMS: Int = 120000)

case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats

case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats

case class MaterializedViewConfig(enabled: Boolean = false, mvName: String = "")

case class PointInTimeReplicationConfig(predicateOp: String = "greaterThan")

case class Replication(allColumns: Boolean = true, columns: List[String] = List(""), useCustomSerializer: Boolean = false, useMaterializedView: MaterializedViewConfig = MaterializedViewConfig(), replicateWithTimestamp: Boolean = false, pointInTimeReplicationConfig: PointInTimeReplicationConfig = PointInTimeReplicationConfig(), readConfiguration: ReadConfiguration = ReadConfiguration())

case class CompressionConfig(enabled: Boolean = false, compressNonPrimaryColumns: List[String] = List(""), compressAllNonPrimaryColumns: Boolean = false, targetNameColumn: String = "")

case class LargeObjectsConfig(enabled: Boolean = false, column: String = "", bucket: String = "", prefix: String = "", enableRefByTimeUUID: Boolean = false, xref: String = "")

case class Transformation(enabled: Boolean = false, addNonPrimaryKeyColumns: List[String] = List(), filterExpression: String = "")

case class UdtConversion(enabled: Boolean = false, columns: List[String] = List(""))

case class Keyspaces(compressionConfig: CompressionConfig, largeObjectsConfig: LargeObjectsConfig, transformation: Transformation, readBeforeWrite: Boolean = false, udtConversion: UdtConversion, writeConfiguration: WriteConfiguration)

case class JsonMapping(replication: Replication, keyspaces: Keyspaces)

case class DlqConfig(s3client: AmazonS3, bucketName: String, key: String)

// Updated CustomResultSetSerializer for Glue 5.0
class CustomResultSetSerializer extends org.json4s.Serializer[com.datastax.oss.driver.api.core.cql.Row] {
  implicit val formats: DefaultFormats.type = DefaultFormats

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
      case DataTypes.TEXT => row.getString(i).replace("'", "''")
      case DataTypes.ASCII => row.getString(i).replace("'", "''")
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

  def binToHex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None =>
        val output = bytes.map("%02x".format(_)).mkString
        s"0x$output"
      case _ =>
        val output = bytes.map("%02x".format(_)).mkString(sep.get)
        s"0x$output"
    }
  }

  override def deserialize(implicit format: org.json4s.Formats): PartialFunction[(org.json4s.TypeInfo, JValue), com.datastax.oss.driver.api.core.cql.Row] = {
    ???
  }
}

// Updated SupportFunctions for Glue 5.0
class SupportFunctions {
  def correctValues(binColumns: List[String] = List(), utdColumns: List[String] = List(), input: String): JValue = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    @tailrec
    def correctEmptyBin(json: JValue, cols: Seq[String]): JValue = cols match {
      case Nil => json
      case col :: tail =>
        val updatedJson = (json \\ col) match {
          case JString("") => json transformField {
            case JField(`col`, _) => JField(col, JString("0x"))
          }
          case _ => json
        }
        correctEmptyBin(updatedJson, tail)
    }

    @tailrec
    def convertUDTtoText(json: JValue, cols: Seq[String]): JValue = cols match {
      case Nil => json
      case col :: tail =>
        val updatedJson = json transformField {
          case JField(`col`, v) => JField(col, JString(s"""${compact(render(v))}"""))
          case other => other
        }
        convertUDTtoText(updatedJson, tail)
    }

    val transformedResult = if (binColumns.isEmpty && utdColumns.isEmpty) {
      parse(input)
    } else {
      val json = parse(input)
      val correctedBins = if (binColumns.nonEmpty) correctEmptyBin(json, binColumns) else json
      val convertedUdt = if (utdColumns.nonEmpty) convertUDTtoText(correctedBins, utdColumns) else correctedBins
      convertedUdt
    }
    transformedResult
  }
}

// Updated FlushingSet for Glue 5.0
case class FlushingSet(flushingClient: CqlSession, internalConfig: WriteConfiguration, dlqConfig: DlqConfig, logger: GlueLogger) {

  lazy val retry: Retry = Retry.of("keyspaces", retryConfig)
  private val batch: BatchStatementBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED)
  private val retryConfig = RetryConfig.custom
    .maxAttempts(internalConfig.maxRetryAttempts)
    .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(internalConfig.expBackoff), 1.1))
    .retryExceptions(
      classOf[WriteFailureException],
      classOf[WriteTimeoutException],
      classOf[ServerError],
      classOf[UnavailableException],
      classOf[NoNodeAvailableException],
      classOf[AllNodesFailedException],
      classOf[DriverException]
    )
    .build()
  private var size: Int = 0

  def executeCQLStatement(statement: String): Unit = {
    logger.info(s"Executing CQL statement: $statement")
    val resTry = Try(Retry.decorateSupplier(retry, () => flushingClient.execute(statement.toString)).get())
    resTry match {
      case Success(_) =>
      case Failure(exception) =>
        logger.error(s"Failed to execute CQL statement after retries: ${exception.getMessage}")
        persistToDlq(dlqConfig, statement.toString)
    }
  }

  private def persistToDlq(dlqConfig: DlqConfig, payload: String): Unit = {
    val ts = java.time.LocalDateTime.now().toString
    val key = s"${dlqConfig.key}/log-$ts.msg"

    try {
      dlqConfig.s3client.putObject(dlqConfig.bucketName, key, payload)
    } catch {
      case ase: AmazonServiceException =>
        logger.error(s"Amazon S3 couldn't process the request: ${ase.getMessage}")
      case sce: SdkClientException =>
        logger.error(s"Couldn't contact Amazon S3 or couldn't parse the response: ${sce.getMessage}")
      case e: Exception =>
        logger.error(s"Unexpected error while persisting to S3: ${e.getMessage}")
    }
  }

  final def add(element: String): Unit = this.synchronized {
    val elementSize = element.toString.getBytes.length
    if (size + elementSize >= internalConfig.batchSize || batch.getStatementsCount > internalConfig.maxStatementsPerBatch) {
      flush()
      size = elementSize
      batch.addStatement(SimpleStatement.newInstance(element.toString))
    } else {
      size += elementSize
      batch.addStatement(SimpleStatement.newInstance(element.toString))
    }
  }

  def flush(): Unit = this.synchronized {
    if (batch.getStatementsCount > 0) {
      executeCQLStatement(batch.build())
      batch.clearStatements()
      size = 0
    }
  }

  private def executeCQLStatement(batchStatement: BatchStatement): Unit = {
    val resTry = Try(Retry.decorateSupplier(retry, () => flushingClient.execute(batchStatement)).get())
    resTry match {
      case Success(_) =>
      case Failure(exception) =>
        logger.error(s"Failed to execute CQL batch statement after retries: ${exception.getMessage}")
        batchStatement.asScala.foreach { stmt =>
          persistToDlq(dlqConfig, stmt.asInstanceOf[SimpleStatement].getQuery)
        }
    }
  }

  def getSize: Int = this.synchronized {
    size
  }
}

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r

    def convertCassandraKeyToGenericKey(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString(":")
    }

    // Updated helper functions for Glue 5.0 compatibility
    def isLogObjectPresent(ksName: String, tblName: String, bucketName: String, s3Client: AmazonS3, tile: Int, op: String): Option[String] = {
      val prefix = s"$ksName/$tblName/dlq/$tile/$op"
      val listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix)
      val objectListing = s3Client.listObjectsV2(listObjectsRequest)
      val firstObjectKeyOption = objectListing.getObjectSummaries.asScala.headOption.map(_.getKey)
      firstObjectKeyOption
    }

    def replayLogs(logger: GlueLogger, ksName: String, tblName: String, bucketName: String, s3Client: AmazonS3, tile: Int, op: String, cc: CqlSession): Unit = {
      val session = cc
      Iterator.continually(isLogObjectPresent(ksName, tblName, bucketName, s3Client, tile, op)).takeWhile(_.nonEmpty).foreach { key =>
        val keyTmp = key.get
        val getObjectRequest = new GetObjectRequest(bucketName, keyTmp)
        val s3Object = s3Client.getObject(getObjectRequest)
        val objectContent = scala.io.Source.fromInputStream(s3Object.getObjectContent).mkString
        Try {
          logger.info(s"Detected a failed $op '$keyTmp' in the dlq. The $op is going to be replayed.")
          session.execute(s"$objectContent IF NOT EXISTS").all()
        } match {
          case Failure(_) => throw new DlqS3Exception(s"Failed to insert $objectContent")
          case Success(_) =>
            s3Client.deleteObject(bucketName, keyTmp)
            logger.info(s"Operation $op '$keyTmp' replayed and removed from the dlq successfully.")
        }
      }
    }

    def readReplicationStatsObject(s3Client: AmazonS3, bucket: String, key: String): ReplicationStats = {
      Try {
        val s3Object = s3Client.getObject(bucket, key)
        val src = Source.fromInputStream(s3Object.getObjectContent)
        val json = src.getLines.mkString
        src.close()
        json
      } match {
        case Failure(_) =>
          ReplicationStats(0, 0, 0, 0, 0, java.time.LocalDateTime.now().toString)
        case Success(json) =>
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(json).extract[ReplicationStats]
      }
    }

    def getDBConnection(connectionConfName: String, bucketName: String, s3client: AmazonS3): CqlSession = {
      val connectorConf = s3client.getObjectAsString(bucketName, s"artifacts/$connectionConfName")
      val connection = CqlSession.builder.withConfigLoader(DriverConfigLoader.fromString(connectorConf))
        .build()
      connection
    }

    def inferKeys(cc: CqlSession, keyType: String, ks: String, tbl: String, columnTs: String): Seq[Map[String, String]] = {
      val meta = cc.getMetadata.getKeyspace(ks).get.getTable(tbl).get
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

    def getAllColumns(cc: CqlSession, ks: String, tbl: String): Seq[scala.collection.immutable.Map[String, String]] = {
      cc.getMetadata.getKeyspace(ks).get().
        getTable(tbl).get().
        getColumns.entrySet.asScala.
        map(x => Map(x.getKey.toString -> x.getValue.getType.toString)).toSeq
    }

    def parseJSONMapping(s: String): JsonMapping = {
      val trimmedString = Option(s).map(_.trim).getOrElse("")
      trimmedString match {
        case "" | "None" | "null" => JsonMapping(Replication(),
          Keyspaces(CompressionConfig(), LargeObjectsConfig(), Transformation(), readBeforeWrite = false, UdtConversion(), WriteConfiguration()))
        case jsonString =>
          Try {
            implicit val formats: DefaultFormats.type = DefaultFormats
            parse(jsonString).extract[JsonMapping]
          } match {
            case Success(mapping) => mapping
            case Failure(ex) =>
              println(s"Failed to parse JSON mapping: ${ex.getMessage}")
              println(s"Input string: '$jsonString'")
              // Return default mapping on parse failure
              JsonMapping(Replication(), Keyspaces(CompressionConfig(), LargeObjectsConfig(), Transformation(), readBeforeWrite = false,
                UdtConversion(), WriteConfiguration()))
          }
      }
    }

    def preFlightCheck(connection: CqlSession, keyspace: String, table: String, dir: String, logger: GlueLogger): Unit = {
      def logErrorAndExit(message: String, code: Int): Nothing = {
        logger.error(message)
        throw new PreFlightCheckException(message, code)
      }

      Try {
        val c1 = Option(connection)
        c1.isEmpty match {
          case false =>
            c1.get.getMetadata.getKeyspace(keyspace).isPresent match {
              case true =>
                c1.get.getMetadata.getKeyspace(keyspace).get.getTable(table).isPresent match {
                  case true =>
                    logger.info(s"the $dir table $table exists")
                  case false =>
                    logErrorAndExit(s"ERROR: the $dir table $table does not exist", -1)
                }
              case false =>
                logErrorAndExit(s"ERROR: the $dir keyspace $keyspace does not exist", -2)
            }
          case _ =>
            logErrorAndExit(s"ERROR: The job was not able to connect to the $dir", -3)
        }
      } match {
        case Failure(r) =>
          val err = s"ERROR: Detected a connectivity issue. Check the conf file. Glue connection for the $dir, the job was aborted"
          logErrorAndExit(s"$err ${r.toString}", -4)
        case Success(_) =>
          logger.info(s"Connected to the $dir")
      }
    }

    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "TILE", "TOTAL_TILES", "PROCESS_TYPE", "SOURCE_KS", "SOURCE_TBL", "TARGET_KS", "TARGET_TBL", "WRITETIME_COLUMN", "TTL_COLUMN", "S3_LANDING_ZONE", "REPLICATION_POINT_IN_TIME", "SAFE_MODE", "CLEANUP_REQUESTED", "JSON_MAPPING", "REPLAY_LOG", "WORKLOAD_TYPE").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val jobRunId = args("JOB_RUN_ID")
    val currentTile = args("TILE").toInt
    val totalTiles = args("TOTAL_TILES").toInt
    val safeMode = args("SAFE_MODE")
    val replayLog = Try(args("REPLAY_LOG").toBoolean).getOrElse(false)
    val workloadTypeArg = Try(args("WORKLOAD_TYPE")).getOrElse("continuous")
    val workloadType = if (workloadTypeArg == "continuous" || workloadTypeArg == "batch") workloadTypeArg
    else {
      logger.error(s"ERROR: Invalid workload type '$workloadTypeArg'. Supported values are 'continuous' and 'batch'")
      sys.exit()
    }

    // Internal configuration
    val WAIT_TIME = safeMode match {
      case "true" => 20000
      case _ => 0
    }

    val internalMigrationKeyspace = "migration"
    val internalMigrationTable = "ledger"
    val SAMPLE_SIZE = 100000
    val SAMPLE_FRACTION = 0.2
    val KEYS_PER_PARQUET_FILE = 10500000
    val cachingMode = safeMode match {
      case "true" => StorageLevel.DISK_ONLY
      case _ => StorageLevel.MEMORY_AND_DISK_SER
    }

    sparkSession.conf.set(s"spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")
    sparkSession.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    val processType = args("PROCESS_TYPE") // discovery or replication
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r

    // Business configuration
    val srcTableName = args("SOURCE_TBL")
    val srcKeyspaceName = args("SOURCE_KS")
    val trgTableName = args("TARGET_TBL")
    val trgKeyspaceName = args("TARGET_KS")
    val landingZone = args("S3_LANDING_ZONE")
    val bcktName = landingZone.replaceAll("s3://", "")
    val columnTs = args("WRITETIME_COLUMN")
    val ttlColumn = args("TTL_COLUMN")
    val jsonMapping = args("JSON_MAPPING")
    val replicationPointInTime = args("REPLICATION_POINT_IN_TIME").toLong
    val cleanUpRequested: Boolean = args("CLEANUP_REQUESTED") match {
      case "false" => false
      case _ => true
    }

    // AmazonS3Client to check if a stop request was issued
    val s3ClientConf = new ClientConfiguration().withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    val s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(s3ClientConf).build()

    val internalConnectionToSource = getDBConnection("CassandraConnector.conf", bcktName, s3client)
    val internalConnectionToTarget = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)

    // Let's do preflight checks
    logger.info("Preflight check started")
    preFlightCheck(internalConnectionToSource, srcKeyspaceName, srcTableName, "source", logger)
    preFlightCheck(internalConnectionToTarget, trgKeyspaceName, trgTableName, "target", logger)
    logger.info("Preflight check completed")

    val pkFinal = columnTs match {
      case "None" => inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
      case _ => inferKeys(internalConnectionToSource, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    }

    val partitionKeys = inferKeys(internalConnectionToSource, "partitionKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    val allColumnsFromSource = getAllColumns(internalConnectionToSource, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList
    val counterColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "COUNTER").keys).toList
    val pkFinalWithoutTs = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val pks = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex
    val jsonMappingRaw = new String(Base64.getDecoder.decode(jsonMapping.replaceAll("\\\\r\\\\n|\\\\r|\\\\n", "")), StandardCharsets.UTF_8)
    val cores: Int = sparkSession.sparkContext.getConf.get("spark.executor.cores").toInt
    val instances: Int = sparkSession.sparkContext.getConf.get("spark.executor.instances").toInt
    val defPar: Int = sparkSession.sparkContext.getConf.get("spark.default.parallelism").toInt
    val defaultPartitions = scala.math.max(defPar, cores * instances * totalTiles)
    logger.info(s"Json mapping: $jsonMappingRaw")
    logger.info(s"Compute resources: Instances $instances, Cores: $cores")

    val jsonMapping4s = parseJSONMapping(jsonMappingRaw.replaceAll("\\\\r\\\\n|\\\\r|\\\\n", ""))
    val replicatedColumns = jsonMapping4s match {
      case JsonMapping(Replication(true, _, _, _, _, _, _), _) => "*"
      case rep => rep.replication.columns.mkString(",")
    }

    def cleanupLedger(cc: CqlSession,
                      logger: GlueLogger,
                      ks: String, tbl: String,
                      cleanUpRequested: Boolean,
                      pt: String): Unit = {
      if (pt.equals("discovery") && cleanUpRequested) {
        cc.execute(s"DELETE FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$ks' and tbl='$tbl'")
        logger.info(s"Cleaned up the $internalMigrationKeyspace.$internalMigrationTable table for $ks.$tbl")
      }
    }

    val udtColumns: List[String] = if (jsonMapping4s.keyspaces.udtConversion.enabled) {
      jsonMapping4s.keyspaces.udtConversion.columns
    } else List.empty

    val cas: String = if (jsonMapping4s.keyspaces.readBeforeWrite) {
      " IF NOT EXISTS"
    } else
      ""

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

    val selectStmtWithTs = columnTs match {
      case s if s.equals("None")
        || !jsonMapping4s.replication.replicateWithTimestamp =>
        replicatedColumns
      case s if !s.equals("None")
        && jsonMapping4s.replication.replicateWithTimestamp
        && replicatedColumns.equals("*") =>
        allColumnsFromSource.flatMap(_.keys) :+ s"writetime($columnTs) as ts" mkString ", "
      case s if !s.equals("None")
        && jsonMapping4s.replication.replicateWithTimestamp
        && !replicatedColumns.equals("*") =>
        s"$replicatedColumns, writetime($columnTs) as ts"
    }

    def getLedgerQueryBuilder(ks: String, tbl: String, tile: Int, ver: String): String = {
      s"SELECT ks, tbl, tile, offload_status, dt_offload, location, ver, load_status, dt_load FROM $internalMigrationKeyspace.$internalMigrationTable " +
        s"WHERE ks='$ks' and tbl='$tbl' and tile=$tile and ver='$ver'"
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
      if (processType == "resize") return false
      val key = processType match {
        case "discovery" => s"$srcKeyspaceName/$srcTableName/$processType/stopRequested"
        case "replication" => s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested"
        case "sampler" => s"$landingZone/$srcKeyspaceName/$srcTableName/columnStats"
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

    def compressValues(json: JValue): JValue = {
      if (jsonMapping4s.keyspaces.compressionConfig.enabled) {
        val jPayload = json
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
        updatedJson
      } else {
        json
      }
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
            org.joda.time.LocalDateTime.now().toString)),
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

    def getTsValue(jValue: org.json4s.JValue): BigInt = {
      val tsJValue = jValue \ "ts"
      tsJValue match {
        case JInt(value) => value
        case _ => -1
      }
    }

    def filterOutColumn(jvalue: JValue, columnName: String): JValue = jvalue match {
      case JObject(fields) =>
        JObject(fields.flatMap {
          case (key, value) if key != columnName =>
            Some(key -> filterOutColumn(value, columnName))
          case _ => None
        })
      case JArray(elements) =>
        JArray(elements.map(filterOutColumn(_, columnName)))
      case other => other
    }

    def backToCQLStatementWithoutTTL(jvalue: org.json4s.JValue): String = {
      val filteredJson = filterOutColumn(jvalue, s"ttl($ttlColumn)")
      compact(render(filteredJson))
    }

    def backToCQLStatementWithoutTs(jvalue: org.json4s.JValue): String = {
      val filteredJson = filterOutColumn(jvalue, "ts")
      compact(render(filteredJson))
    }

    def getSourceRow(cls: String, wc: String, session: CqlSession, defaultFormat: org.json4s.Formats): String = {
      // Validate input parameters
      require(Option(cls).exists(_.trim.nonEmpty), "Column list (cls) cannot be null or empty")
      val query = if (!jsonMapping4s.replication.useCustomSerializer) {
        s"SELECT json $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc"
      } else {
        s"SELECT $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc"
      }

      val emptyResult = ""
      val rs = jsonMapping4s.replication.useCustomSerializer match {
        case false => {
          val row = Option(session.execute(query).one())
          if (row.nonEmpty) {
            row.get.getString(0).replace("'", "''")
          } else
            emptyResult
        }
        case _ => {
          val row = Option(session.execute(query).one())
          if (row.nonEmpty) {
            Serialization.write(row)(defaultFormat)
          } else
            emptyResult
        }
      }
      rs
    }

    def getCounters(row: Row): Map[String, Long] = {
      val listOfCounters = counterColumns.map(
        column => (column, row.getAs[Long](column))
      )
      listOfCounters.iterator.map { case (str, long) => (str, long) }.toMap
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      df.rdd.foreachPartition(partition => {
        lazy val customFormat = if (jsonMapping4s.replication.useCustomSerializer) {
          DefaultFormats + new CustomResultSetSerializer
        } else {
          DefaultFormats
        }
        lazy val supportFunctions = new SupportFunctions()
        lazy val s3ClientOnPartition = AmazonS3ClientBuilder.defaultClient()
        lazy val cassandraConnPerPar = getDBConnection("CassandraConnector.conf", bcktName, s3ClientOnPartition)
        lazy val keyspacesConnPerPar = getDBConnection("KeyspacesConnector.conf", bcktName, s3ClientOnPartition)
        lazy val dlqConfig = DlqConfig(s3ClientOnPartition, bcktName, s"$srcKeyspaceName/$srcTableName/dlq/$tile/$op")
        lazy val fl = FlushingSet(keyspacesConnPerPar, jsonMapping4s.keyspaces.writeConfiguration, dlqConfig, logger)
        lazy val maxStatementsPerBatch = jsonMapping4s.keyspaces.writeConfiguration.maxStatementsPerBatch

        def processRow(row: Row): Unit = {
          val whereClause = rowToStatement(row, columns, columnsPos)
          if (whereClause.nonEmpty) {
            op match {
              case "insert" | "update" =>
                if (counterColumns.nonEmpty) {
                  val counterValues = getCounters(row)
                  val setStmt = counterValues.map(m => s"${m._1}=${m._1}+${m._2}").mkString(",")
                  val counterCqlStmt = s"UPDATE $trgKeyspaceName.$trgTableName SET $setStmt WHERE $whereClause"
                  fl.executeCQLStatement(counterCqlStmt)
                } else {
                  processNonCounterRow(row, whereClause)
                }
              case "delete" =>
                val cqlStatement = s"DELETE FROM $trgKeyspaceName.$trgTableName WHERE $whereClause"
                if (maxStatementsPerBatch > 1)
                  fl.add(cqlStatement)
                else
                  fl.executeCQLStatement(cqlStatement)
              case _ =>
            }
          }
        }

        def processNonCounterRow(row: Row, whereClause: String): Unit = {
          if (ttlColumn.equals("None")) {
            val rs = getSourceRow(selectStmtWithTs, whereClause, cassandraConnPerPar, customFormat)
            if (rs.nonEmpty) {
              processRowWithTimestamp(row, whereClause, rs)
            } else {
              val rs = getSourceRow(selectStmtWithTTL, whereClause, cassandraConnPerPar, customFormat)
              if (rs.nonEmpty) {
                processRowWithTTL(row, whereClause, rs)
              }
            }
          }
        }

        def processRowWithTimestamp(row: Row, whereClause: String, rs: String): Unit = {
          val jsonRowEscaped = supportFunctions.correctValues(blobColumns, udtColumns, rs)
          val jsonRow = compressValues(jsonRowEscaped)
          val json4sRow = jsonRow
          val tsValue = getTsValue(json4sRow)
          val tsSuffix = if (tsValue > 0) s"USING TIMESTAMP $tsValue" else ""
          val backToJsonRow = backToCQLStatementWithoutTs(json4sRow)
          jsonMapping4s.keyspaces.largeObjectsConfig.enabled match {
            case false =>
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' $tsSuffix$cas"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
            case _ =>
              val updatedJsonRow = compact(render(offloadToS3(parse(backToJsonRow), s3ClientOnPartition, whereClause)))
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$updatedJsonRow' $tsSuffix$cas"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
          }
        }

        def processRowWithTTL(row: Row, whereClause: String, rs: String): Unit = {
          val jsonRowEscaped = supportFunctions.correctValues(blobColumns, udtColumns, rs)
          val jsonRow = compressValues(jsonRowEscaped)
          val json4sRow = jsonRow
          jsonMapping4s.keyspaces.largeObjectsConfig.enabled match {
            case false =>
              val backToJsonRow = backToCQLStatementWithoutTTL(json4sRow)
              val ttlVal = getTTLvalue(json4sRow)
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal$cas"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
            case _ =>
              val updatedJsonRow = offloadToS3(json4sRow, s3ClientOnPartition, whereClause)
              val backToJsonRow = backToCQLStatementWithoutTTL(updatedJsonRow)
              val ttlVal = getTTLvalue(json4sRow)
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal$cas"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
          }
        }

        partition.foreach(processRow)
        fl.flush()
        keyspacesConnPerPar.close()
        cassandraConnPerPar.close()
      })
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
          case "string" | "text" | "inet" | "varchar" | "ascii" => s"'${row.getString(position).replace("'", "''")}'"
          case "date" => s"'${row.getDate(position)}'"
          case "timestamp" => {
            val res = row.get(position).getClass.getName match {
              case "java.sql.Timestamp" => {
                val inputFormatterTs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                var originalTs = row.getTimestamp(position).toString
                val dotIndex = originalTs.indexOf(".")
                if (dotIndex != -1 && originalTs.length - dotIndex < 4) {
                  originalTs += "0" * (4 - (originalTs.length - dotIndex))
                }
                val localDateTime = java.time.LocalDateTime.parse(originalTs, inputFormatterTs)
                val zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.of("UTC"))
                s"${zonedDateTime.toInstant.toEpochMilli}"
              }
              case "java.lang.String" => {
                val inputFormatterTs = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                val originalTs = row.getString(position).replace("Z", "+0000")
                val localDateTime = ZonedDateTime.parse(originalTs, inputFormatterTs)
                s"${localDateTime.toInstant.toEpochMilli}"
              }
            }
            res
          }
          case "time" => row.getLong(position)
          case "int" => row.getInt(position)
          case "varint" => row.getAs[java.math.BigDecimal](position)
          case "smallint" => row.getShort(position)
          case "long" | "bigint" => row.getLong(position)
          case "float" => row.getFloat(position)
          case "double" => row.getDouble(position)
          case "decimal" => row.getDecimal(position)
          case "tinyint" => row.getByte(position)
          case "uuid" | "timeuuid" => row.getString(position)
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

    def shuffleDfV2(df: DataFrame): DataFrame = {
      val saltColumnName = java.util.UUID.randomUUID().toString
      val shuffledDf = df.withColumn(s"salt-$saltColumnName", rand())
        .repartition(defaultPartitions, col(s"salt-$saltColumnName"))
        .drop(s"salt-$saltColumnName")
      logger.info(s"Shuffle partitions: ${shuffledDf.rdd.getNumPartitions}")
      shuffledDf
    }

    def persist(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      if (jsonMapping4s.keyspaces.writeConfiguration.preShuffleBeforeWrite) {
        persistToTarget(shuffleDfV2(df), columns, columnsPos, tile, op)
      } else {
        persistToTarget(df.coalesce(defaultPartitions), columns, columnsPos, tile, op)
      }
    }

    def dataReplicationProcess(): Unit = {
      val ledger = internalConnectionToTarget.execute(
        s"""SELECT location,tile,ver
           |FROM $internalMigrationKeyspace.$internalMigrationTable
           | WHERE ks='$srcKeyspaceName'
           |AND tbl='$srcTableName'
           |AND tile=$currentTile
           |AND load_status=''
           |AND offload_status='SUCCESS'
           |AND ver in ('head','tail')
           |ALLOW FILTERING""".stripMargin).all().asScala

      val ledgerList = Option(ledger)

      if (!ledgerList.isEmpty) {
        val locations = ledgerList.get.map(c => (c.getString(0), c.getInt(1), c.getString(2))).toList.par
        val heads = locations.count(_._3 == "head")
        val tails = locations.count(_._3 == "tail")

        if (heads > 0 && tails == 0) {

          logger.info(s"Historical data load.Processing locations: $locations")
          locations.foreach(location => {
            val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)

            val loc = location._1
            val sourcePath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/$loc"
            val sourceDf = glueContext.getSourceWithFormat(
              connectionType = "s3",
              format = "parquet",
              options = JsonOptions(s"""{"paths": ["$sourcePath"]}""")
            ).getDynamicFrame().toDF().persist(cachingMode)

            val sourceDfV2 = sourceDf.drop("group").drop("ts")
            val tile = location._2

            persist(sourceDfV2, columns, columnsPos, tile, "insert")

            ledgerConnection.execute(s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'head','SUCCESS', toTimestamp(now()), '')")
            val cnt = sourceDfV2.count()

            val content = ReplicationStats(tile, cnt, 0, 0, 0, org.joda.time.LocalDateTime.now().toString)
            putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)
            ledgerConnection.close()
            sourceDf.unpersist()
          }
          )
        }

        if ((heads > 0 && tails > 0) || (heads == 0 && tails > 0)) {
          var inserted: Long = 0
          var deleted: Long = 0
          var updated: Long = 0

          logger.info("Processing replica...")
          val pathTail = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.tail"
          val dfTail = glueContext.getSourceWithFormat(
              connectionType = "s3",
              format = "parquet",
              options = JsonOptions(s"""{"paths": ["$pathTail"]}""")
            ).getDynamicFrame()
            .toDF()
            .drop("group")
            .persist(cachingMode)
            .repartition(defaultPartitions, pks.map(c => col(c)): _*)

          val pathHead = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.head"
          val dfHead = glueContext.getSourceWithFormat(
              connectionType = "s3",
              format = "parquet",
              options = JsonOptions(s"""{"paths": ["$pathHead"]}""")
            ).getDynamicFrame()
            .toDF()
            .drop("group").persist(cachingMode)
            .repartition(defaultPartitions, pks.map(c => col(c)): _*)

          val newInsertsDF: DataFrame = dfTail.drop("ts").as("tail").join(dfHead.drop("ts").as("head"), cond, "leftanti").persist(cachingMode)
          val newDeletesDF: DataFrame = dfHead.drop("ts").as("head").join(dfTail.drop("ts").as("tail"), cond, "leftanti").persist(cachingMode)

          columnTs match {
            case ct if ct == "None" && counterColumns.isEmpty => {
              if (!newInsertsDF.isEmpty) {
                persist(newInsertsDF, columns, columnsPos, currentTile, "insert")
                inserted = newInsertsDF.count()
              }
            }
            case ct if ct == "None" && counterColumns.nonEmpty => {
              val newCounterUpdatesDF = dfTail.as("tail").join(dfHead.as("head"), cond, "inner").
                filter($"tail.counter_hash" =!= $"head.counter_hash").
                selectExpr(pks.map(x => s"tail.$x") ++ counterColumns.map(y => s"tail.$y-head.$y as $y"): _*).persist(cachingMode)
              persist(newInsertsDF, columns, columnsPos, currentTile, "insert")
              persist(newCounterUpdatesDF, columns, columnsPos, currentTile, "update")
              inserted = newInsertsDF.count()
              updated = newCounterUpdatesDF.count()
            }
            case _ => {
              val newUpdatesDF = dfTail.as("tail").join(dfHead.as("head"), cond, "inner").
                filter($"tail.ts" > $"head.ts").
                selectExpr(pks.map(x => s"tail.$x"): _*).persist(cachingMode)
              if (!(newInsertsDF.isEmpty && newUpdatesDF.isEmpty)) {
                persist(newInsertsDF, columns, columnsPos, currentTile, "insert")
                persist(newUpdatesDF, columns, columnsPos, currentTile, "update")
                inserted = newInsertsDF.count()
                updated = newUpdatesDF.count()
              }
              newUpdatesDF.unpersist()
            }
          }

          if (!newDeletesDF.isEmpty) {
            persist(newDeletesDF, columns, columnsPos, currentTile, "delete")
            deleted = newDeletesDF.count()
          }

          if (!(updated != 0 && inserted != 0 && deleted != 0)) {
            val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, org.joda.time.LocalDateTime.now().toString)
            putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
          }

          newInsertsDF.unpersist()
          newDeletesDF.unpersist()
          dfTail.unpersist()
          dfHead.unpersist()

          internalConnectionToTarget.execute(s"BEGIN UNLOGGED BATCH " +
            s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'tail','SUCCESS', toTimestamp(now()), '');" +
            s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'head','SUCCESS', toTimestamp(now()), '');" +
            s"APPLY BATCH;")

        }
      }
    }

    def sampleProcess(): Unit = {
      def addTotalColumns(df: DataFrame): DataFrame = {
        // Get all unique column names (without the _name, _min, _avg, _max suffixes)
        val columnNames = df.columns
          .filter(_.endsWith("_name"))
          .map(_.stripSuffix("_name"))

        // Create expressions for the new columns
        val totalExpressions = columnNames.flatMap { colName =>
          Seq(
            coalesce(col(s"${colName}_min").cast(DoubleType), lit(0.0)).as(s"${colName}_min_double"),
            coalesce(col(s"${colName}_avg").cast(DoubleType), lit(0.0)).as(s"${colName}_avg_double"),
            coalesce(col(s"${colName}_max").cast(DoubleType), lit(0.0)).as(s"${colName}_max_double")
          )
        }

        // Add the new columns to the DataFrame
        val dfWithDoubles = df.select(df.columns.map(col) ++ totalExpressions: _*)

        // Calculate totals
        val minTotal = columnNames.map(colName => col(s"${colName}_min_double")).reduce(_ + _)
        val avgTotal = columnNames.map(colName => col(s"${colName}_avg_double")).reduce(_ + _)
        val maxTotal = columnNames.map(colName => col(s"${colName}_max_double")).reduce(_ + _)

        // Add total columns
        dfWithDoubles
          .withColumn("row_min_total", minTotal)
          .withColumn("row_avg_total", avgTotal)
          .withColumn("row_max_total", maxTotal)
          .drop(columnNames.flatMap(colName => Seq(
            s"${colName}_min_double",
            s"${colName}_avg_double",
            s"${colName}_max_double"
          )): _*)
      }

      def getColumnStats(df: DataFrame): DataFrame = {
        val expressions = df.schema.fields.flatMap { field =>
          val colName = field.name
          val col = df(colName)

          val (minExpr, avgExpr, maxExpr) = field.dataType match {
            case _: NumericType | BooleanType =>
              (min(col), avg(col), max(col))
            case StringType =>
              (min(length(col)), avg(length(col)), max(length(col)))
            case BinaryType =>
              (min(length(col)), avg(length(col)), max(length(col)))
            case TimestampType =>
              (lit("8"), lit("8"), lit("8"))
            case DateType =>
              (lit("4"), lit("4"), lit("4"))
            case _ =>
              (lit(null), lit(null), lit(null))
          }

          List(
            lit(colName).alias(s"${colName}_name"),
            minExpr.cast("string").alias(s"${colName}_min"),
            avgExpr.cast("string").alias(s"${colName}_avg"),
            maxExpr.cast("string").alias(s"${colName}_max")
          )
        }
        val columnStats = df.select(expressions: _*)
        columnStats
      }

      val sampledDf = sparkSession.read.cassandraFormat(srcTableName, srcKeyspaceName).option("inferSchema", "true").
        load().limit(SAMPLE_SIZE).sample(SAMPLE_FRACTION)

      val columnStats = addTotalColumns(getColumnStats(sampledDf))
      val jsonStats = parse(columnStats.toJSON.collect().mkString)
      s3client.putObject(bcktName, s"$srcKeyspaceName/$srcTableName/columnStats/stats.json", pretty(render(jsonStats)))
    }

    def sourceScan(): DataFrame = {
      logger.info("Reading the source")
      val cassandraReadConfig = Map(
        "inferSchema" -> "true",
        "spark.cassandra.input.split.sizeInMB" -> jsonMapping4s.replication.readConfiguration.splitSizeInMB.toString,
        "spark.cassandra.concurrent.reads" -> jsonMapping4s.replication.readConfiguration.concurrentReads.toString,
        "spark.cassandra.input.consistency.level" -> jsonMapping4s.replication.readConfiguration.consistencyLevel,
        "spark.cassandra.input.fetch.sizeInRows" -> jsonMapping4s.replication.readConfiguration.fetchSizeInRows.toString,
        "spark.cassandra.query.retry.count" -> jsonMapping4s.replication.readConfiguration.readTimeoutMS.toString,
        "spark.cassandra.read.timeoutMS" -> jsonMapping4s.replication.readConfiguration.readTimeoutMS.toString
      )

      val srcTableForDiscovery = jsonMapping4s.replication.useMaterializedView match {
        case mv if mv.enabled => mv.mvName
        case _ => srcTableName
      }

      val pointInTimePredicate = {
        val op = jsonMapping4s.replication.pointInTimeReplicationConfig.predicateOp
        (c: ColumnName) =>
          op match {
            case "greaterThan" => c > replicationPointInTime
            case "lessThanOrEqual" => c <= replicationPointInTime
            case _ => true
          }
      }

      val filterColumns = jsonMapping4s.keyspaces.transformation.addNonPrimaryKeyColumns match {
        case cols if cols.isEmpty => pkFinal
        case cols => pkFinal ++ cols
      }

      val baseDF = sparkSession.read
        .options(cassandraReadConfig)
        .cassandraFormat(srcTableForDiscovery, srcKeyspaceName)
        .load()

      val primaryKeysDf = columnTs match {
        case ts if ts == "None" && counterColumns.isEmpty =>
          baseDF
            .selectExpr(filterColumns.map(c => c): _*)
            .withColumn("ts", lit(0))

        case ts if ts != "None" && replicationPointInTime == 0 =>
          baseDF
            .selectExpr(filterColumns.map(c => c): _*)

        case ts if ts != "None" && replicationPointInTime > 0 =>
          baseDF
            .selectExpr(filterColumns.map(c => c): _*)
            .filter($"ts".isNotNull && pointInTimePredicate($"ts"))

        case ts if ts == "None" && counterColumns.nonEmpty =>
          val selectedColumns = pkFinal ++ counterColumns
          baseDF
            .selectExpr(selectedColumns.map(c => c): _*)
            .withColumn("ts", lit(0))
            .withColumn("counter_hash", xxhash64(counterColumns.map(c => col(c)): _*))
      }

      val groupingExpr = abs(xxhash64(concat(pkFinalWithoutTs.map(col): _*))) % totalTiles

      val finalDf = primaryKeysDf
        .withColumn("group", groupingExpr)
        .repartition(defaultPartitions, col("group"))
        .transform(df =>
          if (!jsonMapping4s.keyspaces.transformation.enabled) df
          else df.filter(jsonMapping4s.keyspaces.transformation.filterExpression)
        )
      finalDf
    }

    def getCurrentTilesCount(fileType: String = "both"): Int = {
      val listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(bcktName)
        .withPrefix(s"$srcKeyspaceName/$srcTableName/primaryKeysCopy")
      val objectListing = s3client.listObjectsV2(listObjectsRequest)

      // Define pattern based on file type
      val tilePattern = fileType.toLowerCase match {
        case "head" => ".*/tile_(\\d+)\\.head.*".r
        case "tail" => ".*/tile_(\\d+)\\.tail.*".r
        case "both" => ".*/tile_(\\d+)\\.(head|tail).*".r
        case _ => throw new IllegalArgumentException(s"Invalid file type: $fileType. Must be 'head', 'tail', or 'both'")
      }

      val currentNumTiles = objectListing.getObjectSummaries.asScala
        .map(_.getKey)
        .flatMap { key =>
          key match {
            case tilePattern(num, _) => Some(num.toInt)
            case _ => None
          }
        }
        .distinct
        .size

      logger.info(s"Found $currentNumTiles tiles for file type: $fileType")
      currentNumTiles
    }

    def recomputeTiles(): Unit = {
      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        val currentNumTiles = getCurrentTilesCount()
        logger.info(s"new tiles $totalTiles, current tiles $currentNumTiles")

        if (currentNumTiles > 0 && currentNumTiles != totalTiles) {
          logger.info(s"Detected a change in tiles from $currentNumTiles to $totalTiles. Recomputing tiles")

          // Read and transform primary keys
          val primaryKeys = glueContext.getSourceWithFormat(
              connectionType = "s3",
              format = "parquet",
              options = JsonOptions(s"""{"paths": ["$landingZone/$srcKeyspaceName/$srcTableName/primaryKeysCopy"]}""")
            ).getDynamicFrame()
            .toDF()
            .drop("group")
            .distinct()
            .persist(cachingMode)

          // Calculate hash and group
          val hashColumns = pkFinalWithoutTs.map(col)
          val transformedDf = primaryKeys
            .withColumn("group", abs(xxhash64(concat(hashColumns: _*))) % totalTiles)
            .repartition(totalTiles, col("group"))
            .persist(cachingMode)

          // Clear existing ledger entries
          ledgerConnection.execute(s"DELETE FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName'")

          // Process tiles in parallel
          val results = (0 until totalTiles).toList.par.map { tile =>
            try {
              // Process each tile
              val tileDf = transformedDf
                .where(col("group") === tile)
                .repartition(defaultPartitions, pks.map(col): _*)

              // Cache the count before writing
              val tileCount = tileDf.count()

              // Write tile data
              writeWithSizeControl(tileDf, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")

              // Update ledger
              ledgerConnection.execute(
                s"""INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(
                   |ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load
                   |) VALUES(
                   |'$srcKeyspaceName','$srcTableName',$tile,'SUCCESS',toTimestamp(now()),
                   |'tile_$tile.head','head','SUCCESS',''
                   |)""".stripMargin)

              // Update stats
              val content = DiscoveryStats(tile, tileCount, org.joda.time.LocalDateTime.now().toString)
              putStats(
                landingZone.replaceAll("s3://", ""),
                s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile",
                "count.json",
                content
              )
              Right(tile -> tileCount)
            } catch {
              case e: Exception =>
                logger.error(s"Error processing tile $tile: $e")
                Left(tile -> e)
            }
          }.seq

          // Validate results
          val (successes, failures) = results.partition(_.isRight)
          val totalProcessed = successes.collect { case Right((_, count)) => count }.sum
          val originalCount = primaryKeys.count()

          if (failures.nonEmpty) {
            logger.error(s"Failed to process ${failures.size} tiles")
            failures.foreach {
              case Left((tile, error)) =>
                logger.error(s"Tile $tile failed: ${error.getMessage}")
            }
            throw new RuntimeException("Tile processing failed")
          }

          // Verify data integrity
          if (totalProcessed != originalCount) {
            logger.error(s"Data integrity check failed: Original count=$originalCount, Processed count=$totalProcessed")
            throw new RuntimeException("Data integrity check failed")
          }

          // Clean up
          transformedDf.unpersist()
          primaryKeys.unpersist()

          logger.info(s"Tiles are resized successfully. Processed $totalProcessed records")
        } else {
          logger.info("No tiles change detected")
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error during tile re-computation: $e")
          throw e
      } finally {
        ledgerConnection.close()
      }
    }

    def writeWithSizeControl(df: DataFrame, path: String): Unit = {
      df.coalesce(scala.math.max(1, cores * instances))
        .write
        .mode("overwrite")
        .option("maxRecordsPerFile", KEYS_PER_PARQUET_FILE)
        .option("compression", "snappy")
        .save(path)
    }

    def keysDiscoveryProcess(): Unit = {
      val primaryKeysDf = sourceScan().persist(cachingMode)
      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)

      val tiles = (0 until totalTiles).toList.par
      tiles.foreach(tile => {
        val rsTail = ledgerConnection.execute(getLedgerQueryBuilder(srcKeyspaceName, srcTableName, tile, "tail")).one()
        val rsHead = ledgerConnection.execute(getLedgerQueryBuilder(srcKeyspaceName, srcTableName, tile, "head")).one()

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

          val staged = primaryKeysDf.where(col("group") === tile)
          val oldTailPath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail"
          val oldTail = glueContext.getSourceWithFormat(
            connectionType = "s3",
            format = "parquet",
            options = JsonOptions(s"""{"paths": ["$oldTailPath"]}""")
          ).getDynamicFrame().toDF()

          writeWithSizeControl(oldTail, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
          writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")

          ledgerConnection.execute(
            s"BEGIN UNLOGGED BATCH " +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','');" +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','');" +
              s"APPLY BATCH;"
          )
        }

        // The second round (tail and head)
        if (tail.isEmpty && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
          logger.info("Loading a tail but keeping the head")
          val staged = primaryKeysDf.where(col("group") === tile)
          writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
          ledgerConnection.execute(s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
        }

        // Historical upload, the first round (head)
        if (tail.isEmpty && head.isEmpty) {
          logger.info("Loading a head")
          val staged = primaryKeysDf.where(col("group") === tile)
          writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
          ledgerConnection.execute(s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
          val content = DiscoveryStats(tile, staged.count(), org.joda.time.LocalDateTime.now().toString)
          putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
        }
      })
      primaryKeysDf.unpersist()
      ledgerConnection.close()
    }

    cleanupLedger(internalConnectionToTarget, logger, srcKeyspaceName, srcTableName, cleanUpRequested, processType)

    Iterator.continually(stopRequested(bcktName)).takeWhile(_ == false).foreach {
      _ => {
        processType match {
          case "resize" => {
            recomputeTiles()
            sys.exit()
          }
          case "sampler" => {
            sampleProcess()
            sys.exit()
          }
          case "discovery" => {
            keysDiscoveryProcess()
            if (workloadType.equals("batch")) {
              logger.info("The discovery job is completed")
              sys.exit()
            }
          }
          case "replication" => {
            if (replayLog) {
              // Replay inserts
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "insert", internalConnectionToTarget)
              // Replay updates
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "update", internalConnectionToTarget)
              // Replay deletes
              replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "delete", internalConnectionToTarget)
            }
            dataReplicationProcess()
            if (workloadType.equals("batch")) {
              logger.info("The replication job is completed")
              sys.exit()
            }
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