/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon Keyspaces

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
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.Base64
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import net.jpountz.xxhash.{XXHashFactory, XXHash64}

class LargeObjectException(s: String) extends RuntimeException(s)

class ProcessTypeException(s: String) extends RuntimeException(s)

class CassandraTypeException(s: String) extends RuntimeException(s)

class StatsS3Exception(s: String) extends RuntimeException(s)

class CompressionException(s: String) extends RuntimeException(s)

class CustomSerializationException(s: String) extends RuntimeException(s)

class DlqS3Exception(s: String) extends RuntimeException(s)

class PreFlightCheckException(val message: String, val errorCode: Int, val cause: Throwable = null)
  extends Exception(s"[$errorCode] $message", cause) {
  def this(message: String) = this(message, 0)

  def this(message: String, cause: Throwable) = this(message, 0, cause)

  override def toString: String = s"PreFlightCheckException($errorCode, $message)"
}

sealed trait Stats

case class WriteConfiguration(batchSize: Int = 1024 * 1024, maxRetryAttempts: Int = 64, expBackoff: Int = 25, maxStatementsPerBatch: Int = 29, preShuffleBeforeWrite: Boolean = true)

case class ReadConfiguration(splitSizeInMB: Int = 64, concurrentReads: Int = 32, consistencyLevel: String = "LOCAL_ONE", fetchSizeInRows: Int = 500, queryRetryCount: Int = 180,
                             readTimeoutMS: Int = 120000, useCustomVarintReader: Boolean = false)

case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats

case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats

case class MaterializedViewConfig(enabled: Boolean = false, mvName: String = "")

case class PointInTimeReplicationConfig(predicateOp: String = "greaterThan")

case class Replication(allColumns: Boolean = true, columns: List[String] = List(""), useCustomSerializer: Boolean = false, useMaterializedView: MaterializedViewConfig = MaterializedViewConfig(),
                       replicateWithTimestamp: Boolean = false,
                       pointInTimeReplicationConfig: PointInTimeReplicationConfig = PointInTimeReplicationConfig(),
                       readConfiguration: ReadConfiguration = ReadConfiguration())

case class CompressionConfig(enabled: Boolean = false, compressNonPrimaryColumns: List[String] = List(""), compressAllNonPrimaryColumns: Boolean = false, targetNameColumn: String = "")

case class LargeObjectsConfig(enabled: Boolean = false, column: String = "", bucket: String = "", prefix: String = "", enableRefByTimeUUID: Boolean = false, xref: String = "")

case class TransformExpression(columnName: String, rule: String, alias: String = "", keepSource: Boolean = false)

case class Transformation(enabled: Boolean = false, addNonPrimaryKeyColumns: List[String] = List(),
                          filterExpression: String = "",
                          transformExpressions: List[TransformExpression] = List()
                         )

case class UdtConversion(enabled: Boolean = false, columns: List[String] = List(""))

case class Keyspaces(compressionConfig: CompressionConfig, largeObjectsConfig: LargeObjectsConfig, transformation: Transformation, readBeforeWrite: Boolean = false, udtConversion: UdtConversion, writeConfiguration: WriteConfiguration)

case class JsonMapping(replication: Replication, keyspaces: Keyspaces)

case class DlqConfig(s3client: AmazonS3, bucketName: String, key: String)

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

  override def deserialize(implicit format: org.json4s.Formats): PartialFunction[(org.json4s.TypeInfo, JValue), com.datastax.oss.driver.api.core.cql.Row] = {
    ???
  }
}

class SupportFunctions {
  def correctValues(binColumns: List[String] = List(), utdColumns: List[String] = List(), input: String): JValue = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    @tailrec
    def correctEmptyBin(json: JValue, cols: Seq[String]): JValue = cols match {
      case Nil => json
      case col :: tail =>
        val updatedJson = (json \ col) match {
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

case class FlushingSet(flushingClient: CqlSession, internalConfig: WriteConfiguration, dlqConfig: DlqConfig, logger: GlueLogger) {

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

  lazy val retry: Retry = {
    val r = Retry.of("keyspaces", retryConfig)
    r.getEventPublisher.onError(event => {
      logger.error(s"FINAL FAILURE after ${event.getNumberOfRetryAttempts} attempts. " +
        s"Exception: ${event.getLastThrowable.getMessage}")
    })
    r
  }

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
        batchStatement.asScala.foreach {
          stmt =>
            persistToDlq(dlqConfig, stmt.asInstanceOf[SimpleStatement].getQuery)
        }
    }
  }

  private def persistToDlq(dlqConfig: DlqConfig, payload: String): Unit = {
    val ts = org.joda.time.LocalDateTime.now().toString
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
                   cc: CqlSession): Unit = {
      val session = cc
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
            case Failure(_) => throw new DlqS3Exception(s"Failed to insert $objectContent")
            case Success(_) => {
              s3Client.deleteObject(bucketName, keyTmp)
              logger.info(s"Operation $op '$keyTmp' replayed and removed from the dlq successfully.")
            }
          }
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
          ReplicationStats(0, 0, 0, 0, 0, org.joda.time.LocalDateTime.now().toString)
        }
        case Success(json) => {
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(json).extract[ReplicationStats]
        }
      }
    }

    def getDBConnection(connectionConfName: String, bucketName: String, s3client: AmazonS3): CqlSession = {
      val connectorConf = s3client.getObjectAsString(bucketName, s"artifacts/$connectionConfName")
      val connection = CqlSession.builder.withConfigLoader(DriverConfigLoader.fromString(connectorConf))
        .build()
      connection
    }

    def buildWritetimeExpression(columns: Seq[String]): String = columns match {
      case Seq(single) => s"writetime($single) as ts"
      case multiple    => multiple.map(c => s"writetime($c)").mkString("greatest(", ", ", ") as ts")
    }

    def inferKeys(cc: CqlSession, keyType: String, ks: String, tbl: String, columnTs: String, writetimeCols: Seq[String] = Seq.empty): Seq[Map[String, String]] = {
      val meta = cc.getMetadata.getKeyspace(ks).get.getTable(tbl).get
      keyType match {
        case "partitionKeys" =>
          meta.getPartitionKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase))
        case "primaryKeys" =>
          meta.getPrimaryKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase))
        case "primaryKeysWithTS" =>
          meta.getPrimaryKey.asScala.map(x => Map(x.getName.toString -> x.getType.toString.toLowerCase)) :+ Map(buildWritetimeExpression(writetimeCols) -> "bigint")
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
      Option(s) match {
        case Some("None") => JsonMapping(Replication(),
          Keyspaces(CompressionConfig(),
            LargeObjectsConfig(),
            Transformation(),
            readBeforeWrite = false,
            UdtConversion(), WriteConfiguration()))
        case _ =>
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(s).extract[JsonMapping]
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
          case false => {
            c1.get.getMetadata.getKeyspace(keyspace).isPresent match {
              case true => {
                c1.get.getMetadata.getKeyspace(keyspace).get.getTable(table).isPresent match {
                  case true => {
                    logger.info(s"the $dir table $table exists")
                  }
                  case false => {
                    logErrorAndExit(s"ERROR: the $dir table $table does not exist", -1)
                  }
                }
              }
              case false => {
                logErrorAndExit(s"ERROR: the $dir keyspace $keyspace does not exist", -2)
              }
            }
          }
          case _ => {
            logErrorAndExit(s"ERROR: The job was not able to connecto to the $dir", -3)
          }
        }
      } match {
        case Failure(r) => {
          val err = s"ERROR: Detected a connectivity issue. Check the conf file. Glue connection for the $dir, the job was aborted"
          logErrorAndExit(s"$err ${r.toString}", -4)
        }
        case Success(_) => {
          logger.info(s"Connected to the $dir")
        }
      }
    }

    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "TILE", "TOTAL_TILES", "PROCESS_TYPE", "SOURCE_KS", "SOURCE_TBL", "TARGET_KS", "TARGET_TBL", "WRITETIME_COLUMN", "TTL_COLUMN", "S3_LANDING_ZONE", "REPLICATION_POINT_IN_TIME", "SAFE_MODE", "CLEANUP_REQUESTED", "JSON_MAPPING", "REPLAY_LOG", "WORKLOAD_TYPE", "ICEBERG_CATALOG", "LOGGING").toArray)
    val logLevel = args.getOrElse("LOGGING", "ERROR").toUpperCase
    sparkContext.setLogLevel(logLevel)
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

    val internalMigrationKeyspace="migration"
    val internalMigrationTable="ledger"
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
    val icebergCatalog = args("ICEBERG_CATALOG")

    // Iceberg Spark session configuration
    // Note: spark.sql.extensions and spark.sql.catalog.* are static configs
    // and must be set via --conf in the Glue job definition, not at runtime.
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val bcktName = landingZone.replaceAll("s3://", "")
    val columnTs = args("WRITETIME_COLUMN")
    val writetimeColumns: Seq[String] = columnTs match {
      case "None" => Seq.empty
      case csv    => csv.split(",").map(_.trim).toSeq
    }
    val ttlColumn = args("TTL_COLUMN")
    val jsonMapping = args("JSON_MAPPING")
    val replicationPointInTime = args("REPLICATION_POINT_IN_TIME").toLong
    val cleanUpRequested: Boolean = args("CLEANUP_REQUESTED") match {
      case "false" => false
      case _ => true
    }

    //AmazonS3Client to check if a stop request was issued
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
      case _ => inferKeys(internalConnectionToSource, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs, writetimeColumns).flatten.toMap.keys.toSeq
    }

    val pkFinalTarget = inferKeys(internalConnectionToTarget, "primaryKeys", trgKeyspaceName, trgTableName, columnTs).flatten.toMap.keys.toSeq

    val partitionKeys = inferKeys(internalConnectionToSource, "partitionKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    val allColumnsFromSource = getAllColumns(internalConnectionToSource, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList
    val counterColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "COUNTER").keys).toList
    val pkFinalWithoutTs = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
    val pks = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex
    val jsonMappingRaw = new String(Base64.getDecoder.decode(jsonMapping.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    val cores: Int = sparkSession.sparkContext.getConf.get("spark.executor.cores").toInt
    val instances: Int = sparkSession.sparkContext.getConf.get("spark.executor.instances").toInt
    val defPar: Int = sparkSession.sparkContext.getConf.get("spark.default.parallelism").toInt
    val defaultPartitions = scala.math.max(defPar, cores * instances * totalTiles)
    logger.info(s"Json mapping: $jsonMappingRaw")
    logger.info(s"Compute resources: Instances $instances, Cores: $cores")

    val jsonMapping4s = parseJSONMapping(jsonMappingRaw.replaceAll("\\r\\n|\\r|\\n", ""))
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

    val writetimeExpr = buildWritetimeExpression(writetimeColumns)

    val selectStmtWithTs = columnTs match {
      case s if s.equals("None")
        || !jsonMapping4s.replication.replicateWithTimestamp =>
        replicatedColumns
      case s if !s.equals("None")
        && jsonMapping4s.replication.replicateWithTimestamp
        && replicatedColumns.equals("*") =>
        allColumnsFromSource.flatMap(_.keys) :+ writetimeExpr mkString ", "
      case s if !s.equals("None")
        && jsonMapping4s.replication.replicateWithTimestamp
        && !replicatedColumns.equals("*") =>
        s"$replicatedColumns, $writetimeExpr"
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
        lazy val xxHashFactory = XXHashFactory.fastestInstance()
        lazy val xxHash64: XXHash64 = xxHashFactory.hash64()
        lazy val seedXxHash64 = 42L

        def hashValue(value: String, rule: String): Any = {
          rule.toLowerCase match {
            case "murmurhash3" => MurmurHash3.stringHash(value)
            case "md5" =>
              val md = MessageDigest.getInstance("MD5")
              md.digest(value.getBytes(StandardCharsets.UTF_8)).map("%02x".format(_)).mkString
            case "sha-1" | "sha1" =>
              val md = MessageDigest.getInstance("SHA-1")
              md.digest(value.getBytes(StandardCharsets.UTF_8)).map("%02x".format(_)).mkString
            case "sha-2" | "sha2" | "sha-256" | "sha256" =>
              val md = MessageDigest.getInstance("SHA-256")
              md.digest(value.getBytes(StandardCharsets.UTF_8)).map("%02x".format(_)).mkString
            case "xxhash64" =>
              val bytes = value.getBytes("UTF-8")
              xxHash64.hash(bytes, 0, bytes.length, seedXxHash64)
            case _ => value
          }
        }

        def valueTransformer(inputJson: JValue, rules: List[TransformExpression]): JValue = {
          if (rules.isEmpty) return inputJson

          rules.foldLeft(inputJson) { (currentJson, rule) =>
            (currentJson \ rule.columnName) match {
              case JNothing => currentJson
              case value =>
                val stringValue = value.values.toString
                val hashedValue = hashValue(stringValue, rule.rule)
                val newValue = hashedValue match {
                  case s: String => JString(s)
                  case i: Int => JInt(i)
                  case l: Long => JLong(l)
                  case other => JString(other.toString)
                }
                if (rule.alias.nonEmpty) {
                  if (rule.keepSource) {
                    currentJson merge JObject(rule.alias -> newValue)
                  } else {
                    currentJson.removeField { case (k, _) => k == rule.columnName } merge JObject(rule.alias -> newValue)
                  }
                } else {
                  currentJson.transformField { case (k, _) if k == rule.columnName => (k, newValue) }
                }
            }
          }
        }

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
                val deleteWhereClause = if (jsonMapping4s.keyspaces.transformation.enabled && jsonMapping4s.keyspaces.transformation.transformExpressions.nonEmpty) {
                  val whereConditions = scala.collection.mutable.ListBuffer[String]()

                  columnsPos.foreach { el =>
                    val originalColName = el._1
                    if (pkFinalTarget.contains(originalColName)) {
                      val position = row.fieldIndex(originalColName)
                      val originalValue = row.get(position).toString
                      val colType = columns.getOrElse(originalColName, "none")
                      val formattedValue = colType match {
                        case "string" | "text" | "inet" | "varchar" | "ascii" => s"'${originalValue.replace("'", "''")}'"
                        case _ => originalValue
                      }
                      whereConditions += s"$originalColName=$formattedValue"
                    }
                  }

                  jsonMapping4s.keyspaces.transformation.transformExpressions.foreach { rule =>
                    val originalColName = rule.columnName
                    val aliasColName = rule.alias

                    if (columnsPos.exists(_._1 == originalColName) && pkFinalTarget.contains(aliasColName)) {
                      val position = row.fieldIndex(originalColName)
                      val originalValue = row.get(position).toString
                      val hashedValue = hashValue(originalValue, rule.rule).toString

                      val formattedValue = rule.rule.toLowerCase match {
                        case "murmurhash3" => hashedValue // int - no quotes
                        case "xxhash64" => hashedValue // bigint - no quotes
                        case "md5" | "sha-1" | "sha1" | "sha-2" | "sha2" | "sha-256" | "sha256" => s"'${hashedValue.replace("'", "''")}'" // text - with quotes
                        case _ => s"'${hashedValue.replace("'", "''")}'" // default to text
                      }
                      whereConditions += s"$aliasColName=$formattedValue"
                    }
                  }

                  whereConditions.mkString(" and ")
                } else {
                  whereClause
                }
                val cqlStatement = s"DELETE FROM $trgKeyspaceName.$trgTableName WHERE $deleteWhereClause"
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
            }
          } else {
            val rs = getSourceRow(selectStmtWithTTL, whereClause, cassandraConnPerPar, customFormat)
            if (rs.nonEmpty) {
              processRowWithTTL(row, whereClause, rs)
            }
          }
        }

        def processRowWithTimestamp(row: Row, whereClause: String, rs: String): Unit = {
          val jsonRowEscaped = supportFunctions.correctValues(blobColumns, udtColumns, rs)
          val jsonRow = compressValues(jsonRowEscaped)
          val json4sRow = if (jsonMapping4s.keyspaces.transformation.enabled) valueTransformer(jsonRow, jsonMapping4s.keyspaces.transformation.transformExpressions) else jsonRow
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
          val json4sRow = if (jsonMapping4s.keyspaces.transformation.enabled)
            valueTransformer(jsonRow, jsonMapping4s.keyspaces.transformation.transformExpressions)
          else
            jsonRow
          jsonMapping4s.keyspaces.largeObjectsConfig.enabled match {
            case false =>
              val backToJsonRow = backToCQLStatementWithoutTTL(json4sRow)
              val ttlVal = {
                val rawTtl = getTTLvalue(json4sRow)
                if (rawTtl == null) 0 else rawTtl
              }
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow'$cas USING TTL $ttlVal"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
            case _ =>
              val updatedJsonRow = offloadToS3(json4sRow, s3ClientOnPartition, whereClause)
              val backToJsonRow = backToCQLStatementWithoutTTL(updatedJsonRow)
              val ttlVal = {
                val rawTtl = getTTLvalue(json4sRow)
                if (rawTtl == null) 0 else rawTtl
              }
              val cqlStatement = s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow'$cas USING TTL $ttlVal"
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
          case "varint" => {
            // Handle varint - could be BigDecimal or String depending on reader used
            row.get(position) match {
              case bd: java.math.BigDecimal => BigInt(bd.toBigInteger)
              case s: String => s // Already a string representation
              case bi: java.math.BigInteger => bi.toString
              case other =>
                logger.warn(s"Unexpected varint type: ${other.getClass.getName}, value: $other")
                other.toString
            }
          }
          case "smallint" => row.get(position).asInstanceOf[Number].shortValue()
          case "long" | "bigint" => row.getLong(position)
          case "float" => row.getFloat(position)
          case "double" => row.getDouble(position)
          case "decimal" => row.getDecimal(position)
          case "tinyint" => row.get(position).asInstanceOf[Number].byteValue()
          case "uuid" | "timeuuid" => row.getString(position)
          case "boolean" => row.getBoolean(position)
          case "blob" => s"0${lit(row.getAs[Array[Byte]](colName)).toString.toLowerCase.replaceAll("'", "")}"
          case colType if colType.startsWith("list") => listWithSingleQuotes(row.getList[String](position), colType)
          case colType if colType.startsWith("tuple") => {
            val tupleData = row.getAs[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema](position)
            val elements = (0 until tupleData.length).map { i =>
              tupleData.get(i) match {
                case s: String => s"'${s.replace("'", "''")}'"
                case _ => tupleData.get(i).toString
              }
            }.mkString(",")
            s"($elements)"
          }
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
      val shuffledDf = df.withColumn(s"salt-$saltColumnName",rand())
        .repartition(defaultPartitions, col(s"salt-$saltColumnName"))
        .drop(s"salt-$saltColumnName")
      logger.info(s"Shuffle partitions: ${shuffledDf.rdd.getNumPartitions}")
      shuffledDf
    }

    def persist(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Long = {
      val shaped =
        if (jsonMapping4s.keyspaces.writeConfiguration.preShuffleBeforeWrite) shuffleDfV2(df)
        else df.coalesce(defaultPartitions)
      val alreadyCached = shaped.storageLevel != StorageLevel.NONE
      val cached = if (alreadyCached) shaped else shaped.persist(cachingMode)
      try {
        persistToTarget(cached, columns, columnsPos, tile, op)
        cached.count()
      } finally {
        if (!alreadyCached) cached.unpersist(blocking = false)
      }
    }

    // ======================== Iceberg Table Management Functions ========================

    /** Fully qualified Iceberg table name in Glue Catalog, per tile. */
    def icebergTableName(ksName: String, tblName: String, tile: Int): String = {
      s"$icebergCatalog.${ksName}_db.${tblName}_tile_${tile}_pk_snapshots"
    }

    /** Maps Cassandra types to Spark SQL types for Iceberg table creation. */
    def cassandraTypeToSparkSql(cassandraType: String): String = {
      cassandraType.toLowerCase match {
        case "text" | "varchar" | "ascii" | "inet" | "uuid" | "timeuuid" => "STRING"
        case "int" | "varint"                                            => "INT"
        case "bigint" | "counter"                                        => "BIGINT"
        case "float"                                                     => "FLOAT"
        case "double"                                                    => "DOUBLE"
        case "boolean"                                                   => "BOOLEAN"
        case "timestamp"                                                 => "TIMESTAMP"
        case "date"                                                      => "DATE"
        case "decimal"                                                   => "DECIMAL(38,19)"
        case "smallint"                                                  => "SMALLINT"
        case "tinyint"                                                   => "TINYINT"
        case "blob"                                                      => "BINARY"
        case _                                                           => "STRING"
      }
    }

    /**
     * Creates the Iceberg table for a specific tile if it doesn't exist.
     * Schema: primary key columns + ts (bigint)
     * One table per tile for snapshot isolation.
     * Location: s3://{landingZone}/{ks}/{tbl}/iceberg/tile_{tile}/
     */
    def ensureIcebergTableExists(
      spark: SparkSession,
      ksName: String,
      tblName: String,
      tile: Int,
      pkColumns: Seq[Map[String, String]],
      lz: String
    ): Unit = {
      val tableName = icebergTableName(ksName, tblName, tile)
      val columnDefs = pkColumns.map { c =>
        val name = c("name")
        val sparkType = cassandraTypeToSparkSql(c("type"))
        s"$name $sparkType"
      }.mkString(", ")
      val lzPath = lz.stripSuffix("/")
      val location = s"$lzPath/$ksName/$tblName/iceberg/tile_$tile/"
      val createSql =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  $columnDefs,
           |  ts BIGINT
           |) USING iceberg
           |LOCATION '$location'""".stripMargin
      spark.sql(createSql)
    }

    /**
     * Runs expire_snapshots to retain only the N most recent snapshots.
     * Logs and swallows errors to avoid aborting the discovery cycle.
     */
    def expireIcebergSnapshots(
      spark: SparkSession,
      tableName: String,
      retainLast: Int = 2
    ): Unit = {
      Try {
        spark.sql(s"CALL $icebergCatalog.system.expire_snapshots(table => '$tableName', retain_last => $retainLast)")
      } match {
        case Failure(e) => logger.warn(s"Failed to expire snapshots for $tableName: ${e.getMessage}")
        case Success(_) => logger.info(s"Successfully expired snapshots for $tableName, retaining last $retainLast")
      }
    }

    /**
     * Returns the current (latest) snapshot ID of the Iceberg table.
     * Used after a write to record in the ledger.
     */
    def currentIcebergSnapshotId(spark: SparkSession, tableName: String): Option[Long] = {
      Try {
        val snapshots = spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots ORDER BY committed_at DESC LIMIT 1")
        if (snapshots.isEmpty) None
        else Some(snapshots.first().getLong(0))
      } match {
        case Failure(e) =>
          logger.error(s"Failed to get current snapshot ID for $tableName: ${e.getMessage}")
          None
        case Success(result) => result
      }
    }

    /**
     * Checks whether a specific snapshot ID exists in the Iceberg table.
     * Used to validate ledger-referenced snapshots before attempting time-travel reads.
     */
    def snapshotExists(spark: SparkSession, tableName: String, snapshotId: Long): Boolean = {
      Try {
        val result = spark.sql(s"SELECT snapshot_id FROM $tableName.snapshots WHERE snapshot_id = $snapshotId")
        !result.isEmpty
      } match {
        case Failure(e) =>
          logger.warn(s"Failed to check snapshot existence for $snapshotId in $tableName: ${e.getMessage}")
          false
        case Success(exists) => exists
      }
    }

    /**
     * Writes primary keys to the per-tile Iceberg table.
     * Overwrites the entire table content for this tile.
     */
    def writeIcebergTileSnapshot(
      spark: SparkSession,
      tileDf: DataFrame,
      tableName: String
    ): Unit = {
      tileDf.writeTo(tableName).overwritePartitions()
    }

    /**
     * Reads the per-tile Iceberg table at a specific snapshot ID.
     * Uses Spark's snapshot-id read option for time-travel.
     */
    def readIcebergAtSnapshot(
      spark: SparkSession,
      tableName: String,
      snapshotId: Long
    ): DataFrame = {
      spark.read
        .option("snapshot-id", snapshotId.toString)
        .format("iceberg")
        .load(tableName)
    }

    /**
     * Computes the change set between two snapshots for a per-tile table.
     * Returns (inserts, deletes, updates) DataFrames.
     */
    def computeIcebergChanges(
      spark: SparkSession,
      tableName: String,
      prevSnapshotId: Long,
      currSnapshotId: Long,
      pkColumns: Seq[String],
      colTs: String
    ): (DataFrame, DataFrame, DataFrame) = {
      val prev = readIcebergAtSnapshot(spark, tableName, prevSnapshotId)
      val curr = readIcebergAtSnapshot(spark, tableName, currSnapshotId)

      val inserts = curr.join(prev, pkColumns, "leftanti")
      val deletes = prev.join(curr, pkColumns, "leftanti")

      val updates = if (colTs != "None") {
        val joinCols = pkColumns.toSeq
        val joined = curr.alias("curr").join(prev.alias("prev"), joinCols, "inner")
        // Use null-safe comparison: =!= returns null when either side is null,
        // causing rows to be silently dropped. Cassandra writetime() returns
        // null when the tracked column was never written, so we must use
        // NOT (curr.ts <=> prev.ts) to detect null-to-value transitions.
        val filtered = joined.filter(not(col("curr.ts").eqNullSafe(col("prev.ts"))))
        logger.info(s"Update detection: colTs=$colTs, joinCols=$joinCols")
        filtered.select(joinCols.map(c => col(c)) :+ col("curr.ts").alias("ts"): _*)
      } else {
        spark.emptyDataFrame
      }

      (inserts, deletes, updates)
    }

    /**
     * Records a new discovery snapshot in the ledger.
     * Stores the Iceberg snapshot ID in the 'location' field.
     * Rotates the previous 'curr' entry to 'prev' before writing the new 'curr'.
     */
    def recordDiscoverySnapshot(
      session: CqlSession,
      ksName: String,
      tblName: String,
      tile: Int,
      snapshotId: Long
    ): Unit = {
      val existingCurr = Option(session.execute(
        s"SELECT location, load_status FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='curr'"
      ).one())

      existingCurr match {
        case Some(row) =>
          val oldSnapshotId = row.getString("location")
          val oldLoadStatus = Option(row.getString("load_status")).getOrElse("")
          session.execute(
            s"BEGIN UNLOGGED BATCH " +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'$oldSnapshotId','prev','$oldLoadStatus',toTimestamp(now()));" +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','');" +
              s"APPLY BATCH;"
          )
        case None =>
          session.execute(
            s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','')"
          )
      }
    }

    /**
     * Drops a per-tile Iceberg table from the Glue Catalog and cleans up S3 data.
     * Uses Iceberg's built-in DROP TABLE behavior which handles both catalog metadata
     * and underlying S3 data cleanup.
     */
    def dropIcebergTileTable(spark: SparkSession, ksName: String, tblName: String, tile: Int): Unit = {
      val tableName = icebergTableName(ksName, tblName, tile)
      Try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      } match {
        case Failure(e) =>
          logger.warn(s"Failed to drop Iceberg tile table $tableName: ${e.getMessage}")
        case Success(_) =>
          logger.info(s"Dropped Iceberg tile table $tableName")
      }
    }

    /**
     * Retrieves the previous and current snapshot IDs for a tile.
     * Returns (previousSnapshotId, currentSnapshotId) or None if insufficient history.
     */
    def getSnapshotIds(
      session: CqlSession,
      ksName: String,
      tblName: String,
      tile: Int
    ): Option[(Long, Long)] = {
      val prev = Option(session.execute(
        s"SELECT location FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='prev'"
      ).one())

      val curr = Option(session.execute(
        s"SELECT location FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='curr'"
      ).one())

      (prev, curr) match {
        case (Some(prevRow), Some(currRow)) =>
          Try((prevRow.getString("location").toLong, currRow.getString("location").toLong)).toOption
        case _ => None
      }
    }

    /**
     * Marks the current replication cycle as complete for a tile.
     */
    def markReplicationComplete(
      session: CqlSession,
      ksName: String,
      tblName: String,
      tile: Int
    ): Unit = {
      session.execute(
        s"BEGIN UNLOGGED BATCH " +
          s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'prev','SUCCESS',toTimestamp(now()));" +
          s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'curr','SUCCESS',toTimestamp(now()));" +
          s"APPLY BATCH;"
      )
    }

    /**
     * Checks if legacy Parquet head/tail directories exist for a table.
     */
    def hasLegacyParquetData(
      s3Client: com.amazonaws.services.s3.AmazonS3,
      lz: String,
      ksName: String,
      tblName: String,
      numTiles: Int
    ): Boolean = {
      val bucket = lz.replaceAll("s3://", "")
      (0 until numTiles).exists { tile =>
        val headPrefix = s"$ksName/$tblName/primaryKeys/tile_$tile.head/"
        val tailPrefix = s"$ksName/$tblName/primaryKeys/tile_$tile.tail/"
        val headResult = s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucket).withPrefix(headPrefix).withMaxKeys(1))
        val tailResult = s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucket).withPrefix(tailPrefix).withMaxKeys(1))
        headResult.getKeyCount > 0 || tailResult.getKeyCount > 0
      }
    }

    /**
     * Migrates the most recent Parquet snapshot (tail preferred, head fallback)
     * into per-tile Iceberg tables as the initial snapshot.
     */
    def migrateParquetToIceberg(
      spark: SparkSession,
      gc: GlueContext,
      s3Client: com.amazonaws.services.s3.AmazonS3,
      lz: String,
      ksName: String,
      tblName: String,
      numTiles: Int,
      pkColumns: Seq[Map[String, String]]
    ): Unit = {
      val bucket = lz.replaceAll("s3://", "")
      val ledgerConn = getDBConnection("KeyspacesConnector.conf", bucket, s3Client)
      try {
        for (tile <- 0 until numTiles) {
          Try {
            val tableName = icebergTableName(ksName, tblName, tile)
            ensureIcebergTableExists(spark, ksName, tblName, tile, pkColumns, lz)

            val tailPrefix = s"$ksName/$tblName/primaryKeys/tile_$tile.tail/"
            val headPrefix = s"$ksName/$tblName/primaryKeys/tile_$tile.head/"
            val tailResult = s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucket).withPrefix(tailPrefix).withMaxKeys(1))
            val headResult = s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucket).withPrefix(headPrefix).withMaxKeys(1))

            val parquetPath = if (tailResult.getKeyCount > 0) {
              Some(s"s3://$bucket/$ksName/$tblName/primaryKeys/tile_$tile.tail")
            } else if (headResult.getKeyCount > 0) {
              Some(s"s3://$bucket/$ksName/$tblName/primaryKeys/tile_$tile.head")
            } else {
              None
            }

            parquetPath match {
              case Some(path) =>
                val parquetDf = spark.read.parquet(path)
                // Drop 'group' or 'tile' columns — per-tile tables don't need them
                val icebergDf = parquetDf
                  .drop("group")
                  .drop("tile")
                writeIcebergTileSnapshot(spark, icebergDf, tableName)
                currentIcebergSnapshotId(spark, tableName) match {
                  case Some(snapshotId) =>
                    recordDiscoverySnapshot(ledgerConn, ksName, tblName, tile, snapshotId)
                    logger.info(s"Migrated Parquet data for tile $tile from $path to Iceberg with snapshot $snapshotId")
                  case None =>
                    logger.error(s"Failed to get snapshot ID after migrating tile $tile from $path")
                }
              case None =>
                logger.info(s"No legacy Parquet data found for tile $tile, skipping migration")
            }
          } match {
            case Failure(e) =>
              logger.error(s"Failed to migrate Parquet data for tile $tile: ${e.getMessage}")
            case Success(_) =>
          }
        }
      } finally {
        ledgerConn.close()
      }
    }

    // ======================== End Iceberg Functions ========================

    def dataReplicationProcess(): Unit = {
      val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, currentTile)
      val pkColumnNames = pks

      val session = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        logger.info(s"Starting replication for tile $currentTile, Iceberg table: $icebergTblName")

        // Read both curr and prev ledger entries
        val currEntry = Option(session.execute(
          s"SELECT location, load_status, offload_status FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and ver='curr'"
        ).one())

        val prevEntry = Option(session.execute(
          s"SELECT location, load_status, offload_status FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and ver='prev'"
        ).one())

        logger.info(s"Ledger curr for tile $currentTile: ${currEntry.map(r => s"location=${r.getString("location")}, load_status='${r.getString("load_status")}', offload_status='${r.getString("offload_status")}'").getOrElse("NOT FOUND")}")
        logger.info(s"Ledger prev for tile $currentTile: ${prevEntry.map(r => s"location=${r.getString("location")}, load_status='${r.getString("load_status")}', offload_status='${r.getString("offload_status")}'").getOrElse("NOT FOUND")}")

        currEntry match {
          case None =>
            logger.info(s"No pending replication work for tile $currentTile")

          case Some(currRow) =>
            val currOffloadStatus = Option(currRow.getString("offload_status")).getOrElse("")
            val currLoadStatus = Option(currRow.getString("load_status")).getOrElse("")
            val currSnapshotIdOpt = Try(currRow.getString("location").toLong).toOption

            if (currOffloadStatus != "SUCCESS") {
              logger.info(s"Discovery not completed for tile $currentTile (offload_status='$currOffloadStatus')")
            } else if (currSnapshotIdOpt.isEmpty) {
              logger.warn(s"Invalid snapshot ID in ledger for tile $currentTile")
            } else if (currLoadStatus == "SUCCESS") {
              // Already replicated this snapshot, nothing to do
              logger.info(s"Current snapshot already replicated for tile $currentTile")
            } else {
              val currSnapshotId = currSnapshotIdOpt.get

              // Check if prev exists with load_status=SUCCESS — that means we already did
              // an initial load and this is a delta cycle
              val prevSnapshotIdOpt = prevEntry.flatMap(r => Try(r.getString("location").toLong).toOption)
              val prevLoadStatus = prevEntry.map(r => Option(r.getString("load_status")).getOrElse("")).getOrElse("")

              prevSnapshotIdOpt match {
                case Some(prevSnapshotId) if prevLoadStatus == "SUCCESS" =>
                  // Validate both snapshots exist before attempting delta load
                  val prevExists = snapshotExists(sparkSession, icebergTblName, prevSnapshotId)
                  val currExists = snapshotExists(sparkSession, icebergTblName, currSnapshotId)

                  if (!prevExists || !currExists) {
                    logger.warn(s"Snapshot missing for tile $currentTile (prev=$prevSnapshotId exists=$prevExists, curr=$currSnapshotId exists=$currExists), skipping — waiting for discovery to write a fresh snapshot")
                  } else {
                  // Delta load: compute changes between prev and curr snapshots
                  logger.info(s"Processing delta for tile $currentTile (prev=$prevSnapshotId, curr=$currSnapshotId)")
                  var inserted: Long = 0
                  var deleted: Long = 0
                  var updated: Long = 0

                  val (newInsertsDF, newDeletesDF, newUpdatesDF) = computeIcebergChanges(
                    sparkSession, icebergTblName, prevSnapshotId, currSnapshotId,
                    pkColumnNames, columnTs
                  )

                  val insertsDf = newInsertsDF.drop("ts").persist(cachingMode)
                  val deletesDf = newDeletesDF.drop("ts").persist(cachingMode)

                  columnTs match {
                    case ct if ct == "None" && counterColumns.isEmpty => {
                      if (!insertsDf.isEmpty) {
                        inserted = persist(insertsDf, columns, columnsPos, currentTile, "insert")
                      }
                    }
                    case _ => {
                      val updatesDf = newUpdatesDF.drop("ts").persist(cachingMode)
                      if (!(insertsDf.isEmpty && updatesDf.isEmpty)) {
                        inserted = persist(insertsDf, columns, columnsPos, currentTile, "insert")
                        updated = persist(updatesDf, columns, columnsPos, currentTile, "update")
                      }
                      updatesDf.unpersist()
                    }
                  }

                  if (!deletesDf.isEmpty) {
                    deleted = persist(deletesDf, columns, columnsPos, currentTile, "delete")
                  }

                  logger.info(s"Delta completed for tile $currentTile: inserts=$inserted, updates=$updated, deletes=$deleted")

                  if (updated != 0 || inserted != 0 || deleted != 0) {
                    val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, org.joda.time.LocalDateTime.now().toString)
                    putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
                  }

                  insertsDf.unpersist()
                  deletesDf.unpersist()

                  markReplicationComplete(session, srcKeyspaceName, srcTableName, currentTile)
                  expireIcebergSnapshots(sparkSession, icebergTblName)
                  }

                case _ =>
                  // Initial/historical load: no previous replicated snapshot
                  if (!snapshotExists(sparkSession, icebergTblName, currSnapshotId)) {
                    logger.warn(s"Snapshot $currSnapshotId missing for tile $currentTile, skipping — waiting for discovery to write a fresh snapshot")
                  } else {
                  logger.info(s"Historical data load for tile $currentTile (snapshot=$currSnapshotId)")
                  val sourceDf = readIcebergAtSnapshot(sparkSession, icebergTblName, currSnapshotId)
                  val sourceDfV2 = sourceDf.drop("ts")

                  val cnt = persist(sourceDfV2, columns, columnsPos, currentTile, "insert")
                  logger.info(s"Historical load completed for tile $currentTile: $cnt rows inserted")

                  val content = ReplicationStats(currentTile, cnt, 0, 0, 0, org.joda.time.LocalDateTime.now().toString)
                  putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)

                  markReplicationComplete(session, srcKeyspaceName, srcTableName, currentTile)
                  expireIcebergSnapshots(sparkSession, icebergTblName)
                  }
              }
            }
        }
      } finally {
        session.close()
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

      // Check if we need to use custom varint reader
      val useCustomVarintReader = jsonMapping4s.replication.readConfiguration.useCustomVarintReader

      if (useCustomVarintReader) {
        logger.info("Using custom varint reader to handle potential overflow")
        sourceScanWithCustomVarintReader()
      } else {
        sourceScanStandard()
      }
    }

    def sourceScanStandard(): DataFrame = {
      logger.info("Using standard DataFrame reader")
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

      val groupingExpr = abs(xxhash64(concat(
        pkFinalWithoutTs.map { colName =>
          baseDF.schema(colName).dataType match {
            case struct: StructType =>
              // For tuple types, extract fields and concatenate
              val fields = struct.fieldNames.map(field => col(colName).getField(field).cast("string"))
              concat(lit("("), concat_ws(",", fields: _*), lit(")"))
            case _ =>
              col(colName).cast("string")
          }
        }: _*
      ))) % totalTiles

      val finalDf = primaryKeysDf
        .withColumn("group", groupingExpr)
        .repartition(defaultPartitions, col("group"))
        .transform(df =>
          if (!jsonMapping4s.keyspaces.transformation.enabled) df
          else if (jsonMapping4s.keyspaces.transformation.enabled && !jsonMapping4s.keyspaces.transformation.filterExpression.isEmpty)
            df.filter(jsonMapping4s.keyspaces.transformation.filterExpression)
          else df
        )
      finalDf
    }

    def sourceScanWithCustomVarintReader(): DataFrame = {
      import com.datastax.spark.connector._
      import com.datastax.spark.connector.cql.CassandraConnector
      import com.datastax.spark.connector.rdd.ReadConf
      import org.apache.spark.sql.Row
      import sparkSession.implicits._

      logger.info("Using custom varint reader for primary keys")

      val srcTableForDiscovery = jsonMapping4s.replication.useMaterializedView match {
        case mv if mv.enabled => mv.mvName
        case _ => srcTableName
      }

      val pointInTimePredicate = {
        val op = jsonMapping4s.replication.pointInTimeReplicationConfig.predicateOp
        (c: org.apache.spark.sql.ColumnName) =>
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

      // Get column metadata from Cassandra
      val columnMeta = getAllColumns(internalConnectionToSource, srcKeyspaceName, srcTableForDiscovery)
        .map(colMap => colMap.head) // Extract (columnName, columnType) pairs
        .filter(col => filterColumns.contains(col._1))
        .toMap

      logger.info(s"Column metadata: $columnMeta")
      logger.info(s"Filter columns: $filterColumns")

      // Create schema with varint columns as StringType and writetime as LongType
      val schema = StructType(filterColumns.map { colName =>
        // Extract alias for writetime columns
        val fieldName = if (colName.contains("writetime(") && colName.contains(") as ")) {
          val writetimePattern = """writetime\(([^)]+)\)\s+as\s+(.+)""".r
          colName match {
            case writetimePattern(_, alias) => alias.trim
            case _ => colName
          }
        } else {
          colName
        }
        
        val colType = columnMeta.getOrElse(colName, "text").toLowerCase
        val dataType = if (colType == "varint") {
          StringType // Store varint as string to handle overflow
        } else if (colName.contains("writetime(") || colName.contains("as ts")) {
          LongType // Writetime columns return Long values
        } else {
          colType match {
            case "text" | "varchar" | "ascii" => StringType
            case "int" => IntegerType
            case "bigint" => LongType
            case "boolean" => BooleanType
            case "double" => DoubleType
            case "float" => FloatType
            case "timestamp" => TimestampType
            case "date" => DateType
            case "uuid" | "timeuuid" => StringType
            case "blob" => BinaryType
            case "decimal" => DecimalType(38, 0)
            case "smallint" => ShortType
            case "tinyint" => ByteType
            case _ => LongType // Default to LongType for special columns like writetime
          }
        }
        StructField(fieldName, dataType, nullable = true)
      }.toArray)

      logger.info(s"Created schema: ${schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Convert filterColumns to use RDD API syntax for writetime
      val rddSelectColumns = filterColumns.map { colName =>
        if (colName.contains("writetime(") && colName.contains(") as ")) {
          // Extract column name from writetime(column) as alias
          val writetimePattern = """writetime\(([^)]+)\)\s+as\s+(.+)""".r
          colName match {
            case writetimePattern(sourceCol, alias) =>
              logger.info(s"Converting writetime syntax: $colName -> WriteTime($sourceCol) as $alias")
              // For RDD API, we need to use WriteTime function
              com.datastax.spark.connector.WriteTime(sourceCol) as alias
            case _ =>
              com.datastax.spark.connector.ColumnName(colName)
          }
        } else {
          com.datastax.spark.connector.ColumnName(colName)
        }
      }

      logger.info(s"RDD select columns: ${rddSelectColumns.mkString(", ")}")

      // Configure Cassandra connector for RDD API
      val cassandraConnector = CassandraConnector(
        sparkContext.getConf
          .set("spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")
          .set("spark.cassandra.input.consistency.level", jsonMapping4s.replication.readConfiguration.consistencyLevel)
      )
      
      val cassandraRDD = sparkContext.cassandraTable(srcKeyspaceName, srcTableForDiscovery)
        .withConnector(cassandraConnector)
        .withReadConf(ReadConf(
          splitSizeInMB = jsonMapping4s.replication.readConfiguration.splitSizeInMB,
          fetchSizeInRows = jsonMapping4s.replication.readConfiguration.fetchSizeInRows,
          taskMetricsEnabled = true
        ))
        .select(rddSelectColumns: _*)

      logger.info(s"Created Cassandra RDD for table $srcKeyspaceName.$srcTableForDiscovery with custom connector")

      // Convert CassandraRow to Spark Row, handling varint columns specially
      val rowRDD = cassandraRDD.map { cassandraRow =>
        val values = filterColumns.map { colName =>
          val colType = columnMeta.getOrElse(colName, "text").toLowerCase
          if (colType == "varint") {
            // Handle varint as string to avoid overflow using the proper getVarInt method
            try {
              cassandraRow.getVarIntOption(colName) match {
                case Some(varInt) => varInt.toString // Convert to string to avoid overflow and schema mismatch
                case None => null
              }
            } catch {
              case e: Exception =>
                logger.warn(s"Error reading varint column $colName: ${e.getMessage}")
                null
            }
          } else {
            // Handle other column types
            try {
              colType match {
                case "text" | "varchar" | "ascii" => cassandraRow.getStringOption(colName).orNull
                case "int" => cassandraRow.getIntOption(colName).map(Integer.valueOf).orNull
                case "bigint" => cassandraRow.getLongOption(colName).map(java.lang.Long.valueOf).orNull
                case "boolean" => cassandraRow.getBooleanOption(colName).map(java.lang.Boolean.valueOf).orNull
                case "double" => cassandraRow.getDoubleOption(colName).map(java.lang.Double.valueOf).orNull
                case "float" => cassandraRow.getFloatOption(colName).map(java.lang.Float.valueOf).orNull
                case "timestamp" => cassandraRow.getDateOption(colName).orNull
                case "date" => cassandraRow.getDateOption(colName).orNull
                case "uuid" | "timeuuid" => cassandraRow.getUUIDOption(colName).map(_.toString).orNull
                case "blob" => cassandraRow.getBytesOption(colName).map(_.array()).orNull
                case "decimal" => cassandraRow.getDecimalOption(colName).orNull
                case "smallint" => cassandraRow.getIntOption(colName).map(Integer.valueOf).orNull
                case "tinyint" => cassandraRow.getIntOption(colName).map(Integer.valueOf).orNull
                case _ =>
                  // Handle special cases like writetime columns that might return BigInteger
                  val rawValue = cassandraRow.getVarInt(colName)
                  rawValue match {
                    case bi: BigInt => bi.longValue() // Convert BigInteger to Long for writetime
                    case other => other
                  }
              }
            } catch {
              case e: Exception =>
                logger.warn(s"Error reading column $colName of type $colType: ${e.getMessage}")
                null
            }
          }
        }.toSeq
        Row.fromSeq(values)
      }

      val df = sparkSession.createDataFrame(rowRDD, schema)

      val groupingExpr = abs(xxhash64(concat(
        pkFinalWithoutTs.map { colName =>
          col(colName).cast("string")
        }: _*
      ))) % totalTiles

      val finalDf = df
        .withColumn("group", groupingExpr)
        .repartition(defaultPartitions, col("group"))
        .transform(df =>
          if (!jsonMapping4s.keyspaces.transformation.enabled) df
          else if (jsonMapping4s.keyspaces.transformation.enabled && !jsonMapping4s.keyspaces.transformation.filterExpression.isEmpty)
            df.filter(jsonMapping4s.keyspaces.transformation.filterExpression)
          else df
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

    def getCurrentIcebergTilesCount(ledgerSession: CqlSession): Int = {
      val rs = ledgerSession.execute(
        s"SELECT tile FROM $internalMigrationKeyspace.$internalMigrationTable " +
          s"WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName' AND ver='curr' ALLOW FILTERING"
      )
      rs.all().size()
    }

    def recomputeTiles(): Unit = {
      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        // Task 3.1: Use Iceberg-based tile count detection instead of S3 Parquet scan
        val currentNumTiles = getCurrentIcebergTilesCount(ledgerConnection)
        logger.info(s"new tiles $totalTiles, current tiles $currentNumTiles")

        if (currentNumTiles > 0 && currentNumTiles != totalTiles) {
          logger.info(s"Detected a change in tiles from $currentNumTiles to $totalTiles. Recomputing tiles")

          // Task 3.1: Read existing primary keys from per-tile Iceberg tables instead of Parquet
          val tileDataFrames = (0 until currentNumTiles).map { tile =>
            val tblName = icebergTableName(srcKeyspaceName, srcTableName, tile)
            sparkSession.read.format("iceberg").load(tblName)
          }
          val unionedDf = tileDataFrames.reduce(_ unionByName _)
          val primaryKeys = unionedDf
            .drop("tile")
            .distinct()
            .persist(cachingMode)

          val originalCount = primaryKeys.count()

          // Task 3.1: Infer PK columns using inferKeys (same as keysDiscoveryProcess)
          val pkColumnsForIceberg = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs)
            .map(m => { val (name, tpe) = m.head; Map("name" -> name, "type" -> tpe) })

          // Task 3.5: Preserve xxhash64-based grouping logic
          val hashColumns = pkFinalWithoutTs.map(col)
          val transformedDf = primaryKeys
            .withColumn("group", abs(xxhash64(concat(hashColumns: _*))) % totalTiles)
            .repartition(totalTiles, col("group"))
            .persist(cachingMode)

          // Task 3.2: Clear existing ledger entries before writing new tile entries (Req 8.1, 8.2)
          ledgerConnection.execute(s"DELETE FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName'")

          // Task 3.2: Create Iceberg database and ensure all tile tables exist sequentially
          // (same pattern as keysDiscoveryProcess to avoid Glue Catalog concurrency conflicts)
          val dbName = s"$icebergCatalog.${srcKeyspaceName}_db"
          sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
          for (tile <- 0 until totalTiles) {
            ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
          }

          // Task 3.2: Write tiles via Iceberg + record snapshots; Task 3.5: stats reporting
          val results = (0 until totalTiles).toList.par.map { tile =>
            try {
              val tileDf = transformedDf
                .where(col("group") === tile)
                .drop("group")
                .repartition(defaultPartitions, pks.map(col): _*)

              val tileCount = tileDf.count()

              // Write to per-tile Iceberg table
              val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, tile)
              writeIcebergTileSnapshot(sparkSession, tileDf, icebergTblName)

              // Record snapshot in ledger with both curr and prev entries
              // prev is marked as load_status='SUCCESS' so replication sees a valid baseline
              // and does a delta comparison (finding zero changes) instead of a full re-replicate
              val snapshotIdOpt = currentIcebergSnapshotId(sparkSession, icebergTblName)
              snapshotIdOpt match {
                case Some(snapshotId) =>
                  ledgerConnection.execute(
                    s"BEGIN UNLOGGED BATCH " +
                      s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','prev','SUCCESS',toTimestamp(now()));" +
                      s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','');" +
                      s"APPLY BATCH;"
                  )
                  logger.info(s"Recorded snapshot $snapshotId for tile $tile (curr + prev)")
                case None =>
                  logger.warn(s"Could not retrieve snapshot ID after writing tile $tile")
              }

              // Task 3.5: Update stats
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

          // Task 3.4: Validate results — row count integrity check
          val (successes, failures) = results.partition(_.isRight)
          val totalProcessed = successes.collect { case Right((_, count)) => count }.sum

          if (failures.nonEmpty) {
            logger.error(s"Failed to process ${failures.size} tiles")
            failures.foreach {
              case Left((tile, error)) =>
                logger.error(s"Tile $tile failed: ${error.getMessage}")
              case _ => // unreachable — failures only contains Left values
            }
            throw new RuntimeException("Tile processing failed")
          }

          if (totalProcessed != originalCount) {
            logger.error(s"Data integrity check failed: Original count=$originalCount, Processed count=$totalProcessed")
            throw new RuntimeException("Data integrity check failed")
          }

          // Task 3.3: Clean up orphaned Iceberg tiles when tile count decreases
          if (totalTiles < currentNumTiles) {
            for (tile <- totalTiles until currentNumTiles) {
              dropIcebergTileTable(sparkSession, srcKeyspaceName, srcTableName, tile)
              Try {
                ledgerConnection.execute(
                  s"DELETE FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName' AND tile=$tile"
                )
              } match {
                case Failure(e) => logger.warn(s"Failed to delete ledger entries for orphaned tile $tile: ${e.getMessage}")
                case Success(_) => logger.info(s"Deleted ledger entries for orphaned tile $tile")
              }
            }
          }

          // Clean up cached DataFrames
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
      df.coalesce( scala.math.max(1, cores * instances) )
        .write
        .mode("overwrite")
        .option("maxRecordsPerFile", KEYS_PER_PARQUET_FILE)
        .option("compression", "snappy")
        .save(path)
    }

    def keysDiscoveryProcess(): Unit = {
      // Infer PK columns for Iceberg table schema
      val pkColumnsForIceberg = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs)
        .map(m => { val (name, tpe) = m.head; Map("name" -> name, "type" -> tpe) })

      // Migrate legacy Parquet data to Iceberg if present (backward compatibility)
      if (hasLegacyParquetData(s3client, landingZone, srcKeyspaceName, srcTableName, totalTiles)) {
        logger.info(s"Legacy Parquet data detected for $srcKeyspaceName.$srcTableName, migrating to Iceberg")
        migrateParquetToIceberg(sparkSession, glueContext, s3client, landingZone, srcKeyspaceName, srcTableName, totalTiles, pkColumnsForIceberg)
      }

      val primaryKeysDf = sourceScan().persist(cachingMode)
      // Rename 'group' to 'tile' for tile filtering
      val groupedPkDF = primaryKeysDf.withColumnRenamed("group", "tile").persist(cachingMode)

      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        // Create database and all per-tile tables sequentially to avoid Glue Catalog concurrency conflicts
        val dbName = s"$icebergCatalog.${srcKeyspaceName}_db"
        sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
        for (tile <- 0 until totalTiles) {
          ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
        }

        // Data writes can run in parallel — each tile writes to its own independent table
        val tiles = (0 until totalTiles).toList.par
        tiles.foreach(tile => {
          logger.info(s"Processing tile $tile")
          val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, tile)

          // Check if replication has consumed the current snapshot for this tile
          val currEntry = Option(ledgerConnection.execute(
            s"SELECT load_status FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='curr'"
          ).one())
          val currLoadStatus = currEntry.map(r => Option(r.getString("load_status")).getOrElse("")).getOrElse("")

          if (currEntry.isDefined && currLoadStatus != "SUCCESS") {
            logger.info(s"Skipping tile $tile: replication has not yet consumed the current snapshot (load_status='$currLoadStatus')")
          } else {
            val tileDf = groupedPkDF.filter(col("tile") === tile).drop("tile")
            writeIcebergTileSnapshot(sparkSession, tileDf, icebergTblName)
            val snapshotIdOpt = currentIcebergSnapshotId(sparkSession, icebergTblName)
            snapshotIdOpt match {
              case Some(snapshotId) =>
                recordDiscoverySnapshot(ledgerConnection, srcKeyspaceName, srcTableName, tile, snapshotId)
                logger.info(s"Recorded snapshot $snapshotId for tile $tile")
              case None =>
                logger.warn(s"Could not retrieve snapshot ID after writing tile $tile")
            }
            val tileCount = groupedPkDF.where(col("tile") === tile).count()
            val content = DiscoveryStats(tile, tileCount, org.joda.time.LocalDateTime.now().toString)
            putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
          }
        })
      } finally {
        ledgerConnection.close()
      }
      groupedPkDF.unpersist()
      primaryKeysDf.unpersist()
    }

    cleanupLedger(internalConnectionToTarget, logger, srcKeyspaceName, srcTableName, cleanUpRequested, processType)

    // Clean up Iceberg tables and S3 data when --cr flag is used
    if (processType.equals("discovery") && cleanUpRequested) {
      val dbName = s"${srcKeyspaceName}_db"

      // Use fully qualified SDK v2 classes to avoid conflicts with Glue ETL library
      val glueClient = software.amazon.awssdk.services.glue.GlueClient.create()

      try {
        // Delete per-tile Iceberg tables from Glue Catalog
        for (tile <- 0 until totalTiles) {
          val catalogTblName = s"${srcTableName}_tile_${tile}_pk_snapshots"
          Try {
            glueClient.deleteTable(
              software.amazon.awssdk.services.glue.model.DeleteTableRequest.builder()
                .databaseName(dbName).name(catalogTblName).build()
            )
            logger.info(s"Deleted Glue Catalog table $dbName.$catalogTblName")
          } match {
            case Failure(e) => logger.warn(s"Failed to delete Glue Catalog table $dbName.$catalogTblName: ${e.getMessage}")
            case Success(_) =>
          }
        }

        // Clean up S3 Iceberg data
        Try {
          val icebergPrefix = s"$srcKeyspaceName/$srcTableName/iceberg/"
          var listing = s3client.listObjectsV2(new ListObjectsV2Request().withBucketName(bcktName).withPrefix(icebergPrefix))
          listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(bcktName, obj.getKey))
          while (listing.isTruncated) {
            listing = s3client.listObjectsV2(new ListObjectsV2Request().withBucketName(bcktName).withPrefix(icebergPrefix).withContinuationToken(listing.getNextContinuationToken))
            listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(bcktName, obj.getKey))
          }
          logger.info(s"Cleaned up S3 Iceberg data at s3://$bcktName/$icebergPrefix")
        } match {
          case Failure(e) => logger.warn(s"Failed to clean up S3 Iceberg data: ${e.getMessage}")
          case Success(_) =>
        }

        // Delete the database from Glue Catalog
        Try {
          glueClient.deleteDatabase(
            software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest.builder()
              .name(dbName).build()
          )
          logger.info(s"Deleted Glue Catalog database $dbName")
        } match {
          case Failure(e) => logger.warn(s"Failed to delete Glue Catalog database $dbName: ${e.getMessage}")
          case Success(_) =>
        }
      } finally {
        glueClient.close()
      }
    }

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
