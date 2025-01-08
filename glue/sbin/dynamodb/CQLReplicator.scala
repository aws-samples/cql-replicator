/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon DynamoDB

import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.retry.{PredefinedRetryPolicies, RetryPolicy}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Base64
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

sealed abstract class BaseException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause) {
  println(s"$message - $cause")
}

sealed abstract class OperationalException(message: String) extends BaseException(message) {
  def this() = this("An error occurred")
}

// Exceptions for specific error cases
case class LargeObjectException(message: String) extends OperationalException(message)
case class ProcessTypeException(message: String) extends OperationalException(message)
case class StatsS3Exception(message: String) extends OperationalException(message)
case class CompressionException(message: String) extends OperationalException(message)
case class CustomSerializationException(message: String) extends OperationalException(message)
case class DlqS3Exception(message: String) extends OperationalException(message)
case class RecomputeTilesException(message: String) extends OperationalException(message)

sealed abstract class DataException(message: String, cause: Throwable = null) extends BaseException(message, cause)
case class CassandraTypeException(message: String, cause: Throwable = null) extends DataException(message, cause)
case class DynamoDBTypeException(message: String, cause: Throwable = null) extends DataException(message, cause)
case class DynamoDbOperationException(message: String, cause: Throwable = null) extends DataException(message, cause)
case class UnsupportedFeatures(message: String, cause: Throwable = null) extends DataException(message, cause)

// Special case for PreFlightCheckException
case class PreFlightCheckException(message: String, errorCode: Int = 0, cause: Throwable = null) extends BaseException(s"[$errorCode] $message", cause) {
  override def toString: String = s"PreFlightCheckException($errorCode, $message)"
}

sealed trait Stats
case class DynamoDBConnection(endpoint: String, region: String)
case class WriteConfiguration(writeBatchSize: Int = 1024 * 1024, batchSize: Int = 1024 * 1024, maxRetryAttempts: Int = 64, maxStatementsPerBatch: Int = 24, preShuffleBeforeWrite: Boolean = true, backoff: Int = 25)
case class ReadConfiguration(splitSizeInMB: Int = 64, concurrentReads: Int = 32, consistencyLevel: String = "LOCAL_ONE", fetchSizeInRows: Int = 500, queryRetryCount: Int = 180,
                             readTimeoutMS: Int = 120000)
case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats
case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats
case class MaterializedViewConfig(enabled: Boolean = false, mvName: String = "")
case class PointInTimeReplicationConfig(predicateOp: String = "greaterThan")
case class DynamoDBPrimaryKey(partitionKeyName: String = "PK", sortKeyName: String = "", separator: String = "#")
case class Replication(allColumns: Boolean = true, columns: List[String] = List(""), useCustomSerializer: Boolean = false, useMaterializedView: MaterializedViewConfig = MaterializedViewConfig(),
                       replicateWithTimestamp: Boolean = false,
                       pointInTimeReplicationConfig: PointInTimeReplicationConfig = PointInTimeReplicationConfig(),
                       readConfiguration: ReadConfiguration = ReadConfiguration(),
                       dynamoDBPrimaryKey: DynamoDBPrimaryKey = DynamoDBPrimaryKey())
case class CompressionConfig(enabled: Boolean = false, compressNonPrimaryColumns: List[String] = List(""), compressAllNonPrimaryKeysColumns: Boolean = false, targetAttributeName: String = "")
case class LargeObjectsConfig(enabled: Boolean = false, columns: List[String] = List(""), bucket: String = "", prefix: String = "", keySeparator: String = "#", compressionEnabled: Boolean = false)
case class Transformation(enabled: Boolean = false, addNonPrimaryKeyColumns: List[String] = List(), filterExpression: String = "")
case class UdtConversion(enabled: Boolean = true, columns: List[String] = List(""))
case class DDB(dynamoDBConnection: DynamoDBConnection, compressionConfig: CompressionConfig, largeObjectsConfig: LargeObjectsConfig, transformation: Transformation, readBeforeWrite: Boolean = false, udtConversion: UdtConversion, writeConfiguration: WriteConfiguration)
case class JsonMapping(replication: Replication, dynamodb: DDB)

class RowConverter {
  // Pre-compile patterns and formatters
  private val TimestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val IsoTimestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private val UTC = ZoneId.of("UTC")

  // Cache common string patterns
  private val StringTypes = Set("string", "text", "inet", "varchar", "ascii")
  private val NumericTypes = Set("int", "long", "bigint", "float", "double", "decimal", "smallint", "tinyint", "varint")

  def listWithSingleQuotes(lst: java.util.List[String], colType: String): String = {
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r
    colType match {
      case patternForSingleQuotes(_*) => lst.asScala.toList.map(c => s"'$c'").mkString("[", ",", "]")
      case _ => lst.asScala.toList.map(c => s"$c").mkString("[", ",", "]")
    }
  }

  def rowToStatement(row: Row, columns: Map[String, String], columnsPos: Seq[(String, Int)]): (String, mutable.LinkedHashMap[String, AttributeValue]) = {
    val whereStmt = new StringBuilder(columns.size * 20) // Pre-allocate buffer size
    val attributes = new mutable.LinkedHashMap[String, AttributeValue]()

    columnsPos.foreach { el =>
      val colName = el._1
      val position = row.fieldIndex(el._1)
      val colType: String = columns.getOrElse(el._1, "none")
      val sourceValue: String = convertValue(row, position, colName, colType)
      val targetValue: Map[String, AttributeValue] = convertDDBValue(row, position, colName, colType)

      // Cassandra format: colName=value
      targetValue.foreach { case (key, value) =>
        attributes.put(key, value)  // Use put to maintain insertion order
      }

      whereStmt
        .append(colName)
        .append('=')
        .append(sourceValue)

      el._2 match {
        case i if i < columns.size - 1 =>
          whereStmt.append(" and ")
        case _ =>
      }
    }
    (whereStmt.toString(), attributes)
  }

  // Use it only for primary keys
  private def convertDDBValue(row: Row, position: Int, colName: String, colType: String): Map[String, AttributeValue] = {
    try {
      colType match {
        case t if StringTypes.contains(t) =>
          Map(colName -> new AttributeValue().withS(row.getString(position)))
        case "date" =>
          Map(colName -> new AttributeValue().withS(row.getDate(position).toString))
        case "timestamp" =>
          Map(colName -> new AttributeValue().withN(handleTimestamp(row, position)))
        case "time" =>
          Map(colName -> new AttributeValue().withN(row.getLong(position).toString))
        case t if NumericTypes.contains(t) =>
          Map(colName -> new AttributeValue().withN(getNumericValue(row, position, t)))
        case "uuid" | "timeuuid" =>
          Map(colName -> new AttributeValue().withS(row.getString(position)))
        case "boolean" =>
          Map(colName -> new AttributeValue().withBOOL(row.getBoolean(position)))
        case "blob" =>
          Map(colName -> new AttributeValue().withB(ByteBuffer.wrap(row.getAs[Array[Byte]](colName))))
        case _ =>
          throw CassandraTypeException(s"Unrecognized data type $colType")
      }
    } catch {
      case e: Exception =>
        throw DynamoDBTypeException(
          s"Error converting column $colName of type $colType: $e, position: $position, row: ${row.json}", e)
    }
  }

  private def convertValue(row: Row, position: Int, colName: String, colType: String): String = {
    try {
      colType match {
        case t if StringTypes.contains(t) =>
          formatString(row.getString(position))
        case "date" => formatString(row.getDate(position).toString)
        case "timestamp" =>
          handleTimestamp(row, position)
        case "time" =>
          row.getLong(position).toString
        case t if NumericTypes.contains(t) =>
          getNumericValue(row, position, t)
        case "uuid" | "timeuuid" =>
          row.getString(position)
        case "boolean" =>
          row.getBoolean(position).toString
        case "blob" =>
          formatBlob(row, colName)
        case t if t.startsWith("list") =>
          listWithSingleQuotes(row.getList[String](position), t)
        case _ =>
          throw CassandraTypeException(s"Unrecognized data type $colType")
      }
    } catch {
      case e: Exception =>
        throw CassandraTypeException(
          s"Error converting column $colName of type $colType: $e, position: $position, row: ${row.json}", e)
    }
  }
  private def formatString(value: String): String = {
    s"'${value.replace("'", "''")}'"
  }
  private def handleTimestamp(row: Row, position: Int): String = {
    row.get(position).getClass.getName match {
      case "java.sql.Timestamp" =>
        val originalTs = row.getTimestamp(position).toString
        val normalizedTs = normalizeTimestamp(originalTs)
        val localDateTime = java.time.LocalDateTime.parse(normalizedTs, TimestampFormatter)
        val zonedDateTime = ZonedDateTime.of(localDateTime, UTC)
        zonedDateTime.toInstant.toEpochMilli.toString
      case "java.lang.String" =>
        val originalTs = row.getString(position).replace("Z", "+0000")
        val zonedDateTime = ZonedDateTime.parse(originalTs, IsoTimestampFormatter)
        zonedDateTime.toInstant.toEpochMilli.toString
    }
  }
  private def normalizeTimestamp(ts: String): String = {
    val dotIndex = ts.indexOf('.')
    if (dotIndex != -1 && ts.length - dotIndex < 4) {
      ts + "0" * (4 - (ts.length - dotIndex))
    } else {
      ts
    }
  }
  private def getNumericValue(row: Row, position: Int, colType: String): String = {
    colType match {
      case "int" => row.getInt(position).toString
      case "long" | "bigint" => row.getLong(position).toString
      case "float" => row.getFloat(position).toString
      case "double" => row.getDouble(position).toString
      case "decimal" => row.getDecimal(position).toString
      case "smallint" => row.getShort(position).toString
      case "tinyint" => row.getByte(position).toString
      case "varint" => row.getAs[java.math.BigDecimal](position).toString
    }
  }
  private def formatBlob(row: Row, colName: String): String = {
    s"0${lit(row.getAs[Array[Byte]](colName)).toString.toLowerCase.replaceAll("'", "")}"
  }
}

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
      case _ => throw CustomSerializationException(s"Unsupported data type: $dataType")
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
  def correctValues(binColumns: List[String] = List(), utdColumns: List[String] = List(), input: String): String = {
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
      input
    } else {
      val json = parse(input)
      val correctedBins = if (binColumns.nonEmpty) correctEmptyBin(json, binColumns) else json
      val convertedUdt = if (utdColumns.nonEmpty) convertUDTtoText(correctedBins, utdColumns) else correctedBins
      compact(render(convertedUdt))
    }
    transformedResult
  }
}

case class FlushingSet(flushingClient: AmazonDynamoDB,internalConfig: WriteConfiguration,
                       logger: GlueLogger, targetTable: String) {

  private val statements: ListBuffer[WriteRequest] = ListBuffer.empty

  final def add(element: WriteRequest): Unit = this.synchronized {
    if ( statements.size > internalConfig.maxStatementsPerBatch ) {
      flush()
      statements.append(element)
    } else {
      statements.append(element)
    }
  }

  def executeSinglePut(writeRequest: WriteRequest): PutItemResult = {
    val putRequest = writeRequest.getPutRequest
    val request = new PutItemRequest()
      .withTableName(targetTable)
      .withItem(putRequest.getItem)
    flushingClient.putItem(request)
  }

  def executeSingleDelete(writeRequest: WriteRequest): DeleteItemResult = {
    val deleteRequest = writeRequest.getDeleteRequest
    val request = new DeleteItemRequest()
      .withTableName(targetTable)
      .withKey(deleteRequest.getKey)
    flushingClient.deleteItem(request)
  }

  def flush(): Unit = this.synchronized {
    if (statements.nonEmpty) {
      val requestItems = Map(targetTable -> statements.asJava).asJava
      val batchRequest = new BatchWriteItemRequest().withRequestItems(requestItems)
      var result = flushingClient.batchWriteItem(batchRequest)
      var unprocessed = result.getUnprocessedItems
      var backoff = internalConfig.backoff.toLong
      var retryCount = 0

      while (!unprocessed.isEmpty && retryCount < internalConfig.maxRetryAttempts) {
        result = flushingClient.batchWriteItem(unprocessed)
        unprocessed = result.getUnprocessedItems
        backoff = Math.min(backoff * 2, 64000L)
        retryCount += 1
      }
      statements.clear()
    }
  }
}

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r

    def convertCassandraKeyToGenericKey(input: String, separator: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString(separator)
    }

    def cleanupLedger(cc: CqlSession,
                      logger: GlueLogger,
                      ks: String, tbl: String,
                      cleanUpRequested: Boolean,
                      pt: String): Unit = {
      if (pt.equals("discovery") && cleanUpRequested) {
        cc.execute(s"DELETE FROM migration.ledger WHERE ks='$ks' and tbl='$tbl'")
        logger.info("Cleaned up the migration.ledger")
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
      CqlSession.builder.withConfigLoader(DriverConfigLoader.fromString(connectorConf))
        .build()
    }

    def inferKeys(cc: CqlSession, keyType: String, ks: String, tbl: String, columnTs: String): Seq[mutable.LinkedHashMap[String, String]] = {
      val meta = cc.getMetadata.getKeyspace(ks).get.getTable(tbl).get

      // Get partition key columns in their defined order
      val partitionKeyColumns = meta.getPartitionKey.asScala

      // Get clustering key columns in their defined order
      val clusteringColumns = meta.getClusteringColumns.asScala.keys

      // Combine them in the correct order: partition keys first, then clustering keys
      val orderedPrimaryKeys = partitionKeyColumns ++ clusteringColumns

      keyType match {
        case "partitionKeys" =>
          partitionKeyColumns.map(x => mutable.LinkedHashMap(x.getName.toString -> x.getType.toString.toLowerCase))

        case "primaryKeys" =>
          orderedPrimaryKeys.map(x => mutable.LinkedHashMap(x.getName.toString -> x.getType.toString.toLowerCase))

        case "primaryKeysWithTS" =>
          orderedPrimaryKeys.map(x => mutable.LinkedHashMap(x.getName.toString -> x.getType.toString.toLowerCase)) :+
            mutable.LinkedHashMap(s"writetime($columnTs) as ts" -> "bigint")

        case _ =>
          orderedPrimaryKeys.map(x => mutable.LinkedHashMap(x.getName.toString -> x.getType.toString.toLowerCase))
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
          DDB(
            DynamoDBConnection("", ""),
            CompressionConfig(),
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
        throw PreFlightCheckException(message, code)
      }

      Try {
        dir match {
          case "target" => {
            val dynamoClient = AmazonDynamoDBClientBuilder.standard().build()
            try {
              dynamoClient.describeTable(table)
              logger.info(s"the $dir table $table exists")
            } catch {
              case _: ResourceNotFoundException =>
                logErrorAndExit(s"ERROR: the $dir table $table does not exist", -1)
              case e: Exception =>
                logErrorAndExit(s"ERROR: Failed to check DynamoDB table: ${e.getMessage}", -4)
            } finally {
              dynamoClient.shutdown()
            }
          }
          case _ => {
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
                logErrorAndExit(s"ERROR: The job was not able to connect to the $dir", -3)
              }
            }
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
      case "None" => inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).map(_.keys.head)
      case _ => inferKeys(internalConnectionToSource, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs).map(_.keys.head)
    }

    val stringTypes = Set("STRING", "TEXT", "INET", "VARCHAR", "ASCII", "UUID", "TIMEUUID", "DATE", "TIME", "TIMESTAMP")
    val numericTypes = Set("INT", "LONG", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "SMALLINT", "TINYINT", "VARINT", "COUNTER")
    val booleanTypes = Set("BOOL", "BOOLEAN")
    val binaryTypes = Set("BLOB", "BINARY")
    val collectionListNumerical = Set("LIST(INT, NOT FROZEN)", "LIST(LONG, NOT FROZEN)", "LIST(BIGINT, NOT FROZEN)")
    val collectionListString = Set("LIST(TEXT, NOT FROZEN)", "LIST(VARCHAR, NOT FROZEN)", "LIST(ASCII, NOT FROZEN)", "LIST(UUID, NOT FROZEN)", "LIST(TIMEUUID, NOT FROZEN)", "LIST(DATE, NOT FROZEN)", "LIST(TIME, NOT FROZEN)", "LIST(TIMESTAMP, NOT FROZEN)", "LIST(INET, NOT FROZEN)")
    val collectionMapStringToNumerical = Set("MAP(TEXT => INT, NOT FROZEN)")
    val collectionMapNumericalToString = Set("MAP(INT => TEXT, NOT FROZEN)")
    val collectionMapStringToString = Set("MAP(TEXT => TEXT, NOT FROZEN)")

    def mapCassandraTypeToDDB(columns: Seq[Map[String, String]], excludedColumns: List[String]): Map[String, String] = {
      def processColumn(colMap: Map[String, String]): (String, String) = {
        val (colName, colType) = colMap.head // since each map has only one entry
        val normalizedType = colType.toUpperCase

        val mappedType = normalizedType match {
          case t if stringTypes.contains(t) => "string"
          case t if t.startsWith("UDT") => "string"
          case t if numericTypes.contains(t) => "numeric"
          case t if binaryTypes.contains(t) => "binary"
          case t if booleanTypes.contains(t) => "boolean"
          case t if collectionListNumerical.contains(t) => "arrayofnumerical"
          case t if collectionListString.contains(t) => "arrayofstrings"
          case t if collectionMapStringToNumerical.contains(t) => "mapstringtonumerical"
          case t if collectionMapNumericalToString.contains(t) => "mapnumericaltostring"
          case t if collectionMapStringToString.contains(t) => "mapstringtostring"
          case _ => throw DynamoDBTypeException(s"Unsupported type: $colType for column: $colName")
        }
        colName -> mappedType
      }

      columns
        .filter(colMap => !excludedColumns.contains(colMap.head._1)) // Filter out excluded columns
        .map(processColumn).toMap
    }

    val partitionKeys = inferKeys(internalConnectionToSource, "partitionKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    val allColumnsFromSource = getAllColumns(internalConnectionToSource, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList
    val counterColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "COUNTER").keys).toList
    val pkFinalWithoutTs = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val clusteringColumns = pkFinalWithoutTs.filterNot(partitionKeys.contains)
    val pks = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = columns.keys.toSeq.zipWithIndex
    val jsonMappingRaw = new String(Base64.getDecoder.decode(jsonMapping.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    val cores: Int = sparkSession.sparkContext.getConf.get("spark.executor.cores").toInt
    val instances: Int = sparkSession.sparkContext.getConf.get("spark.executor.instances").toInt
    val defPar: Int = sparkSession.sparkContext.getConf.get("spark.default.parallelism").toInt
    val defaultPartitions = scala.math.max(defPar, cores * instances * totalTiles)
    logger.info(s"Json mapping: $jsonMappingRaw")
    logger.info(s"Compute resources: Instances $instances, Cores: $cores")
    val jsonMapping4s = parseJSONMapping(jsonMappingRaw.replaceAll("\\r\\n|\\r|\\n", ""))
    val replicatedColumns = jsonMapping4s match {
      case JsonMapping(Replication(true, _, _, _, _, _, _, _), _) => "*"
      case rep => rep.replication.columns.mkString(",")
    }

    def getDDBConnection(endpoint: String, region: String): AmazonDynamoDB = {

      if (endpoint.isEmpty || region.isEmpty) {
        throw DynamoDbOperationException("Endpoint and region must not be empty")
      }

      val clientConfiguration = new ClientConfiguration()
        .withRetryPolicy(
          new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
            PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
            jsonMapping4s.dynamodb.writeConfiguration.maxRetryAttempts,
            true
          )
        )

      val clientBuilder = AmazonDynamoDBClientBuilder.standard()
        .withClientConfiguration(clientConfiguration)

      if (endpoint.nonEmpty && region.nonEmpty) {
        val endpointConfig = new EndpointConfiguration(endpoint, region)
        clientBuilder.withEndpointConfiguration(endpointConfig)
      } else {
        clientBuilder.withRegion(region)
      }

      clientBuilder.build()
    }


    val excludedColumns = if (jsonMapping4s.dynamodb.largeObjectsConfig.enabled) {
      jsonMapping4s.dynamodb.largeObjectsConfig.columns
    } else List.empty

    val targetCompressionDDBAttribute = if (jsonMapping4s.dynamodb.compressionConfig.enabled) {
      Seq(Map(jsonMapping4s.dynamodb.compressionConfig.targetAttributeName -> "BLOB"))
    } else Seq.empty[Map[String, String]]

    val ddbTtlAttribute = if (ttlColumn != "None") {
      val ddbConnection = getDDBConnection(jsonMapping4s.dynamodb.dynamoDBConnection.endpoint,
        jsonMapping4s.dynamodb.dynamoDBConnection.region)
      val request = new DescribeTimeToLiveRequest()
        .withTableName(trgTableName)
      val result = ddbConnection.describeTimeToLive(request)
      val ttlDescription = result.getTimeToLiveDescription
      ddbConnection.shutdown()
      Seq(Map(ttlDescription.getAttributeName -> "BIGINT"))
    } else Seq.empty[Map[String, String]]

    val ddbAttributes = mapCassandraTypeToDDB(
      allColumnsFromSource ++ targetCompressionDDBAttribute ++ ddbTtlAttribute,
      excludedColumns)

    def measureTime[T](name: String, thresholdSeconds: Double = 30.0)(fn: => T): T = {
      val startTime = System.currentTimeMillis()
      try {
        fn // execute the passed function
      } finally {
        val endTime = System.currentTimeMillis()
        val elapsedSeconds = (endTime - startTime) / 1000.0
        if (elapsedSeconds > thresholdSeconds) {
          logger.info(s"$name completed in $elapsedSeconds seconds")
        }
      }
    }

    val udtColumns: List[String] = if (jsonMapping4s.dynamodb.udtConversion.enabled) {
      jsonMapping4s.dynamodb.udtConversion.columns
    } else List.empty

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

    val selectStmtWithTs = {
      if (replicatedColumns.isEmpty) {
        throw DynamoDbOperationException("replicatedColumns cannot be empty")
      }

      columnTs match {
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
    }

    def getLedgerQueryBuilder(ks: String, tbl: String, tile: Int, ver: String): String = {
      s"SELECT ks, tbl, tile, offload_status, dt_offload, location, ver, load_status, dt_load FROM migration.ledger " +
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
        case Failure(e) => throw CompressionException(s"Unable to compress due to $e")
        case Success(res) => res
      }
    }

    def stopRequested(bucket: String): Boolean = {
      if (processType == "resize") return false
      val key = processType match {
        case "discovery" => s"$srcKeyspaceName/$srcTableName/$processType/stopRequested"
        case "replication" => s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested"
        case "sampler" => s"$landingZone/$srcKeyspaceName/$srcTableName/columnStats"
        case _ => throw ProcessTypeException("Unrecognizable process type")
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

    def deleteFromS3(s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3, whereClause: String): Unit = {
      val localConfig = jsonMapping4s.dynamodb.largeObjectsConfig
      val bucket = removeS3prefixIfPresent(localConfig.bucket)
      val prefix = removeLeadingEndingSlashes(localConfig.prefix)
      val targetPrefix = s"$prefix/key=${convertCassandraKeyToGenericKey(whereClause, jsonMapping4s.dynamodb.largeObjectsConfig.keySeparator)}/payload"
      Try {
        s3ClientOnPartition.deleteObject(bucket, targetPrefix)
      } match {
        case Failure(r) => throw LargeObjectException(s"Not able to delete the large object to the S3 bucket due to $r")
        case Success(_) =>
      }
    }

    def offloadToS3(jPayload: org.json4s.JValue, s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3, whereClause: String): JValue = {
      val localConfig = jsonMapping4s.dynamodb.largeObjectsConfig
      val columnNames = localConfig.columns
      val bucket = removeS3prefixIfPresent(localConfig.bucket)
      val prefix = removeLeadingEndingSlashes(localConfig.prefix)
      val largeObject = if (localConfig.compressionEnabled) Base64.getEncoder.encodeToString(
        compressWithLZ4B(
          compact(
            JObject(
              columnNames.map(col =>
                JField(col, jPayload \ col)
              )
            )
          )
        )
      ) else {
        compact(
          JObject(
            columnNames.map(col =>
              JField(col, jPayload \ col)
            )
          )
        )
      }

      val updatedJsonStatement =
        // Remove all specified columns
        columnNames.foldLeft(jPayload) { (acc, columnName) =>
          acc removeField {
            case JField(`columnName`, _) => true
            case _ => false
          }
        }

      val targetPrefix = s"$prefix/key=${convertCassandraKeyToGenericKey(whereClause, jsonMapping4s.dynamodb.largeObjectsConfig.keySeparator)}/payload"

      Try {
        s3ClientOnPartition.putObject(bucket, targetPrefix, largeObject)
      } match {
        case Failure(r) => throw LargeObjectException(s"Not able to persist the large object to the S3 bucket due to $r")
        case Success(_) => updatedJsonStatement
      }
    }

    def compressValues(json: String): String = {
      if (jsonMapping4s.dynamodb.compressionConfig.enabled) {
        val jPayload = parse(json)

        val compressColumns = jsonMapping4s.dynamodb.compressionConfig.compressAllNonPrimaryKeysColumns match {
          case true => {
            val acfs = allColumnsFromSource.flatMap(_.keys).toSet
            val excludePks = columns.map(x => x._1.toString).toSet
            acfs.filterNot(excludePks.contains(_))
          }
          case _ => jsonMapping4s.dynamodb.compressionConfig.compressNonPrimaryColumns.toSet
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
            filteredJson merge JObject(jsonMapping4s.dynamodb.compressionConfig.targetAttributeName -> JString(binToHex(compressedPayload)))
          }
          case _ => throw CompressionException("Compressed payload is empty")
        }
        compact(render(updatedJson))
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
        case _ => throw StatsS3Exception("Unknown stats type")
      }
      Try {
        s3client.putObject(bucket, s"$key/$objectName", newContent)
      } match {
        case Failure(_) => throw StatsS3Exception(s"Can't persist the stats to the S3 bucket $bucket")
        case Success(_) => logger.info(message)
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

    def updateTTL(jValue: JValue): JValue = {
      if (!ttlColumn.equals("None")) {
        val newField = ddbTtlAttribute.head.head._1
        val valueByKey: BigInt = (jValue \ s"ttl($ttlColumn)") match {
          case JInt(values) => values
          case _ => 0
        }
        jValue.transform {
          case JObject(fields) => JObject(
            fields.filterNot(_._1 == s"ttl($ttlColumn)") :+ (newField -> JInt(valueByKey))
          )
        }
      } else
        jValue
    }

    def backToCQLStatementWithoutTs(jvalue: org.json4s.JValue): JValue = {
      val filteredValue = filterOutColumn(jvalue, "ts")
      updateTTL(filteredValue)
    }

    def getSourceRow(cls: String = "*", wc: String, session: CqlSession, defaultFormat: org.json4s.Formats): String = {
      // Validate input parameters
      require(Option(cls).exists(_.trim.nonEmpty), "Column list (cls) cannot be null or empty")

      val query = if (!jsonMapping4s.replication.useCustomSerializer) {
        s"SELECT json $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc"
      } else {
        s"SELECT $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc"
      }

      val emptyResult = ""
      val rs = if (jsonMapping4s.replication.useCustomSerializer) {
        val row = Option(session.execute(query).one())
        if (row.nonEmpty) {
          Serialization.write(row)(defaultFormat)
        } else
          emptyResult
      } else {
        val row = Option(session.execute(query).one())
        if (row.nonEmpty) {
          row.get.getString(0).replace("'", "''")
        } else
          emptyResult
      }
      rs
    }

    def createAttributeValue(value: JValue, dataType: String): AttributeValue = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      if (value == JNull) {
        new AttributeValue().withNULL(true)
      } else {
        dataType.toLowerCase match {
          case "string" =>
            new AttributeValue().withS(value.extract[String])
          case "numeric" =>
            value match {
              case JInt(n) => new AttributeValue().withN(n.toString)
              case JDouble(n) => new AttributeValue().withN(n.toString)
              case JLong(n) => new AttributeValue().withN(n.toString)
              case JDecimal(n) => new AttributeValue().withN(n.toString)
              case JString(n) => new AttributeValue().withN(n)
              case _ => throw DynamoDBTypeException(s"Invalid numeric value: $value")
            }
          case "binary" =>
            value match {
              case JString(hexStr) =>
                if (hexStr.toLowerCase.startsWith("0x")) {
                  val hex = hexStr.substring(2)
                  val bytes = hex.sliding(2, 2).map(h => Integer.parseInt(h, 16).toByte).toArray
                  new AttributeValue().withB(java.nio.ByteBuffer.wrap(bytes))
                } else {
                  throw DynamoDBTypeException(s"Binary value must start with '0x': $hexStr")
                }
              case _ => throw DynamoDBTypeException(s"Invalid binary value: $value")
            }
          case "boolean" =>
            new AttributeValue().withBOOL(value.extract[Boolean])

          case "arrayofnumerical" =>
            value match {
              case JArray(elements) =>
                val numberSet = elements.map {
                  case JInt(n) => n.toString
                  case JDouble(n) => n.toString
                  case JLong(n) => n.toString
                  case JDecimal(n) => n.toString
                  case JString(n) => n
                  case _ => throw DynamoDBTypeException(s"Invalid numeric value in array: $value")
                }
                new AttributeValue().withNS(numberSet.asJava)
              case _ => throw DynamoDBTypeException(s"Invalid number array value: $value")
            }

          case "arrayofstrings" =>
            value match {
              case JArray(elements) =>
                val stringSet = elements.map {
                  case JString(s) => s
                  case _ => throw DynamoDBTypeException(s"Invalid string value in array: $value")
                }
                new AttributeValue().withSS(stringSet.asJava)
              case _ => throw DynamoDBTypeException(s"Invalid string array value: $value")
            }

          case "mapstringtonumerical" =>
            value match {
              case JObject(fields) =>
                val map = fields.map { case (key, value) =>
                  value match {
                    case JInt(n) => key -> new AttributeValue().withN(n.toString)
                    case JDouble(n) => key -> new AttributeValue().withN(n.toString)
                    case JLong(n) => key -> new AttributeValue().withN(n.toString)
                    case JDecimal(n) => key -> new AttributeValue().withN(n.toString)
                    case JString(n) => key -> new AttributeValue().withN(n)
                    case _ => throw DynamoDBTypeException(s"Invalid numeric value in map: $value")
                  }
                }.toMap
                new AttributeValue().withM(map.asJava)
              case _ => throw DynamoDBTypeException(s"Invalid map value: $value")
            }

          case "mapnumericaltostring" =>
            value match {
              case JObject(fields) =>
                val map = fields.map { case (key, value) =>
                  value match {
                    case JString(s) => key -> new AttributeValue().withS(s)
                    case _ => throw DynamoDBTypeException(s"Invalid string value in map: $value")
                  }
                }.toMap
                new AttributeValue().withM(map.asJava)
              case _ => throw DynamoDBTypeException(s"Invalid map value: $value")
            }

          case "mapstringtostring" =>
            value match {
              case JObject(fields) =>
                val map = fields.map { case (key, value) =>
                  value match {
                    case JString(s) => key -> new AttributeValue().withS(s)
                    case _ => throw DynamoDBTypeException(s"Invalid string value in map: $value")
                  }
                }.toMap
                new AttributeValue().withM(map.asJava)
              case _ => throw DynamoDBTypeException(s"Invalid map value: $value")
            }

          case _ => throw DynamoDBTypeException(s"Unsupported data type: $dataType")
        }
      }
    }


    def createPutRequest(
                          json: JValue,
                          partitionKeys: List[String],
                          sortKeys: List[String],
                          dataTypes: Map[String, String],
                          separator: String,
                          targetPartitionKeyName: String,
                          targetSortKeyName: String
                        ): WriteRequest = {
      implicit val formats: DefaultFormats.type = DefaultFormats

      // Handle partition key(s)
      val partitionKeyAttribute = if (partitionKeys.size == 1) {
        // Single partition key - use original type
        val key = partitionKeys.head
        val dataType = dataTypes.getOrElse(key,
          throw DynamoDBTypeException(s"Single partition key - use original type. Missing data type for partition key: $key"))

        (json \ key) match {
          case JNothing => throw DynamoDBTypeException(s"Single partition key - use original type. Missing partition key: $key")
          case JNull => throw DynamoDBTypeException(s"Single partition key - use original type. Partition key cannot be null: $key")
          case value => createAttributeValue(value, dataType)
        }
      } else {
        // Multiple partition keys - combine as String
        val combinedKey = partitionKeys.map { key =>
          (json \ key) match {
            case JNothing => throw DynamoDBTypeException(s"Multiple partition keys - combine as String. Missing partition key: $key")
            case JNull => throw DynamoDBTypeException(s"Multiple partition keys - combine as String. Partition key cannot be null: $key")
            case value => value.extract[String]
          }
        }.mkString(separator)

        new AttributeValue().withS(combinedKey)
      }

      // Handle sort key(s) if present
      val itemWithSortKey = if (sortKeys.nonEmpty) {
        val sortKeyAttribute = if (sortKeys.size == 1) {
          // Single sort key - use original type
          val key = sortKeys.head
          val dataType = dataTypes.getOrElse(key,
            throw DynamoDBTypeException(s"Handle sort key(s) if present. Missing data type for sort key: $key"))

          (json \ key) match {
            case JNothing => throw DynamoDBTypeException(s"Handle sort key(s) if present. Missing sort key: $key")
            case JNull => throw DynamoDBTypeException(s"Handle sort key(s) if present. Sort key cannot be null: $key")
            case value => createAttributeValue(value, dataType)
          }
        } else {
          // Multiple sort keys - combine as String
          val combinedKey = sortKeys.map { key =>
            (json \ key) match {
              case JNothing => throw DynamoDBTypeException(s"Multiple sort keys - combine as String. Missing sort key: $key")
              case JNull => throw DynamoDBTypeException(s"Multiple sort keys - combine as String. Sort key cannot be null: $key")
              case value => value.extract[String]
            }
          }.mkString(separator)

          new AttributeValue().withS(combinedKey)
        }
        Map(targetSortKeyName -> sortKeyAttribute)
      } else {
        Map.empty[String, AttributeValue]
      }

      val attributesToExclude = if (jsonMapping4s.dynamodb.compressionConfig.enabled) {
        if (jsonMapping4s.dynamodb.compressionConfig.compressAllNonPrimaryKeysColumns) {
          val acfs = allColumnsFromSource.flatMap(_.keys).toSet
          val excludePks = columns.keys.toSet
          acfs.diff(excludePks)
        } else {
          jsonMapping4s.dynamodb.compressionConfig.compressNonPrimaryColumns
        }
      } else {
        Set.empty[String]
      }

      // Process remaining attributes
      val itemWithoutKeys = dataTypes.map { case (key, dataType) =>
        if ((partitionKeys ++ sortKeys ++ attributesToExclude).contains(key)) {
          None
        } else {
          val value = (json \ key) match {
            case JNothing => throw DynamoDBTypeException(s"Missing field: $key")
            case x => x
          }
          Some(key -> createAttributeValue(value, dataType))
        }
      }.flatten.toMap

      // Combine all attributes
      val item = itemWithoutKeys ++
        Map(targetPartitionKeyName -> partitionKeyAttribute) ++
        itemWithSortKey

      new WriteRequest(new PutRequest().withItem(item.asJava))
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: Seq[(String, Int)], tile: Int, op: String): Unit = {
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
        lazy val dynamodbCnnParPar = getDDBConnection(jsonMapping4s.dynamodb.dynamoDBConnection.endpoint, jsonMapping4s.dynamodb.dynamoDBConnection.region)
        lazy val fl = FlushingSet(dynamodbCnnParPar, jsonMapping4s.dynamodb.writeConfiguration,logger, trgTableName)
        lazy val maxStatementsPerBatch = jsonMapping4s.dynamodb.writeConfiguration.maxStatementsPerBatch
        lazy val rowConverter = new RowConverter
        lazy val ddbPartitionKey = jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyName
        lazy val ddbSortKey = jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyName
        lazy val ddbKeySeparator = jsonMapping4s.replication.dynamoDBPrimaryKey.separator

        def processRow(row: Row): Unit = {
          val wc = rowConverter.rowToStatement(row, columns, columnsPos)
          val whereClause = wc._1
          val attributes = wc._2
          if (whereClause.nonEmpty) {
            if (counterColumns.nonEmpty && counterColumns.size > 1) {
              throw UnsupportedFeatures("Unsupported feature. The tool doesn't support multiple counters")
            } else {
              prepareAndExecute(whereClause, attributes, op)
            }
          }
        }

        def prepareAndExecute(whereClause: String, attributes: mutable.LinkedHashMap[String, AttributeValue], op: String): Unit = {
          val selectStmt = if (selectStmtWithTs.isEmpty) "*" else selectStmtWithTs
          if (op == "insert" || op == "update") {
            val rs = if (ttlColumn.equals("None"))
              getSourceRow(selectStmt, whereClause, cassandraConnPerPar, customFormat)
            else
              getSourceRow(selectStmtWithTTL, whereClause, cassandraConnPerPar, customFormat)
            executePut(rs, whereClause)
          }
          else if (op == "delete") {
            executeDelete(attributes, whereClause)
          }
        }

        def executeDelete(attributes: mutable.LinkedHashMap[String, AttributeValue], wc: String): Unit = {

          def combineKeys(keys: Seq[String]): String = {
            attributes.filterKeys(keys.contains)
              .values
              .map(av =>
                Option(av.getS).orElse(Option(av.getN)).getOrElse("")
              )
              .mkString(ddbKeySeparator)
          }

          def createDeleteRequest(partitionValue: AttributeValue, sortValue: Option[AttributeValue] = None): WriteRequest = {
            val keyMap = sortValue match {
              case Some(sortKey) => Map(
                ddbPartitionKey -> partitionValue,
                ddbSortKey -> sortKey
              )
              case None => Map(
                ddbPartitionKey -> partitionValue
              )
            }
            new WriteRequest(new DeleteRequest().withKey(keyMap.asJava))
          }

          Try {
            (partitionKeys.size, clusteringColumns.size) match {
              // Single partition key, no clustering columns
              case (1, 0) =>
                fl.executeSingleDelete(createDeleteRequest(attributes.values.head))

              // Single partition key, single clustering column
              case (1, 1) =>
                fl.executeSingleDelete(createDeleteRequest(
                  attributes(ddbPartitionKey),
                  Some(attributes(clusteringColumns.head))
                ))

              // Multiple partition keys and single clustering column
              case (n, 1) if n > 1 =>
                val partitionValue = new AttributeValue().withS(combineKeys(partitionKeys))
                fl.executeSingleDelete(createDeleteRequest(
                  partitionValue,
                  Some(attributes(clusteringColumns.head))
                ))

              // Single partition key, multiple clustering columns
              case (1, n) if n > 1 =>
                val sortKeyValue = new AttributeValue().withS(combineKeys(clusteringColumns))
                fl.executeSingleDelete(createDeleteRequest(
                  attributes(ddbPartitionKey),
                  Some(sortKeyValue)
                ))

              // Multiple partition keys, no clustering columns
              case (_, 0) /*if attributes.size > 1 */ =>
                val partitionValue = new AttributeValue().withS(combineKeys(partitionKeys))
                fl.executeSingleDelete(createDeleteRequest(partitionValue))

              // Multiple partition keys, multiple clustering columns
              case (_, n) if n > 1 /*&& attributes.size > 1 */ =>
                val partitionValue = new AttributeValue().withS(combineKeys(partitionKeys))
                val sortKeyValue = new AttributeValue().withS(combineKeys(clusteringColumns))
                fl.executeSingleDelete(createDeleteRequest(
                  partitionValue,
                  Some(sortKeyValue)
                ))

              case _ => throw DynamoDbOperationException("Unsupported key configuration")
            }
          } match {
            case Failure(e) => throw DynamoDbOperationException(s"Failed to delete the item $wc", e)
            case Success(_) => if (jsonMapping4s.dynamodb.largeObjectsConfig.enabled) deleteFromS3(s3ClientOnPartition, wc)
          }
        }

        def executePut(rs: String, whereClause: String): Unit = {
          val jsonRowEscaped = supportFunctions.correctValues(blobColumns, udtColumns, rs)
          val jsonRow = compressValues(jsonRowEscaped)
          val json4sRow = parse(jsonRow)
          val backToJsonRow = backToCQLStatementWithoutTs(json4sRow)

          val ddbRequest = createPutRequest(backToJsonRow,
            partitionKeys.toList,
            clusteringColumns.toList,
            ddbAttributes,
            ddbKeySeparator,
            ddbPartitionKey,
            ddbSortKey
          )

          if (maxStatementsPerBatch > 1 && !jsonMapping4s.dynamodb.largeObjectsConfig.enabled) {
            fl.add(ddbRequest)
          } else if ( (maxStatementsPerBatch > 1 || maxStatementsPerBatch == 1) && jsonMapping4s.dynamodb.largeObjectsConfig.enabled) {
            fl.executeSinglePut(ddbRequest)
            offloadToS3(backToJsonRow, s3ClientOnPartition, whereClause)
          } else if (maxStatementsPerBatch == 1 && !jsonMapping4s.dynamodb.largeObjectsConfig.enabled) {
            fl.executeSinglePut(ddbRequest)
          }
        }

        partition.foreach(processRow)
        fl.flush()
        keyspacesConnPerPar.close()
        cassandraConnPerPar.close()
        dynamodbCnnParPar.shutdown()
      })
    }

    def shuffleDfV2(df: DataFrame): DataFrame = {
      val saltColumnName = java.util.UUID.randomUUID().toString
      val shuffledDf = df.withColumn(s"salt-$saltColumnName",rand())
        .repartition(defaultPartitions, col(s"salt-$saltColumnName"))
        .drop(s"salt-$saltColumnName")
      logger.info(s"Shuffle partitions: ${shuffledDf.rdd.getNumPartitions}")
      shuffledDf
    }

    def persist(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: Seq[(String, Int)], tile: Int, op: String): Unit = {
      if (jsonMapping4s.dynamodb.writeConfiguration.preShuffleBeforeWrite) {
        persistToTarget(shuffleDfV2(df), columns, columnsPos, tile, op)
      } else {
        persistToTarget(df.coalesce(defaultPartitions), columns, columnsPos, tile, op)
      }
    }

    def dataReplicationProcess(): Unit = {
      val ledger = internalConnectionToTarget.execute(
        s"""SELECT location,tile,ver
           |FROM migration.ledger
           |WHERE ks='$srcKeyspaceName'
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

            ledgerConnection.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'head','SUCCESS', toTimestamp(now()), '')")
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
            .repartition(defaultPartitions, pks.map(c => col(c)): _*)
            .persist(cachingMode)

          val pathHead = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.head"
          val dfHead = glueContext.getSourceWithFormat(
              connectionType = "s3",
              format = "parquet",
              options = JsonOptions(s"""{"paths": ["$pathHead"]}""")
            ).getDynamicFrame()
            .toDF()
            .drop("group")
            .repartition(defaultPartitions, pks.map(c => col(c)): _*)
            .persist(cachingMode)

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

              val tailKeys = dfTail
                .select(col("ts") +: pks.map(col): _*)
                .withColumn("tail_ts", col("ts"))
                .drop("ts")
                .persist(cachingMode)

              val headKeys = dfHead
                .select(col("ts") +: pks.map(col): _*)
                .withColumn("head_ts", col("ts"))
                .drop("ts")
                .persist(cachingMode)

              val tsComparisonDF = tailKeys
                .join(broadcast(headKeys), pks, "inner")
                .filter($"tail_ts" > $"head_ts")
                .select(pks.map(col): _*)
                .persist(cachingMode)

              if (!newInsertsDF.isEmpty) {
                persist(newInsertsDF, columns, columnsPos, currentTile, "insert")
                inserted = newInsertsDF.count()
              }

              if (!tsComparisonDF.isEmpty) {
                val newUpdatesDF = dfTail
                  .join(broadcast(tsComparisonDF), pks, "inner")
                  .persist(cachingMode)
                persist(newUpdatesDF, columns, columnsPos, currentTile, "update")
                updated = newUpdatesDF.count()
                newUpdatesDF.unpersist()

              }
              tailKeys.unpersist()
              headKeys.unpersist()
              tsComparisonDF.unpersist()
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
            s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'tail','SUCCESS', toTimestamp(now()), '');" +
            s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'head','SUCCESS', toTimestamp(now()), '');" +
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

      val filterColumns = jsonMapping4s.dynamodb.transformation.addNonPrimaryKeyColumns match {
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
          if (!jsonMapping4s.dynamodb.transformation.enabled) df
          else df.filter(jsonMapping4s.dynamodb.transformation.filterExpression)
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
          ledgerConnection.execute(s"DELETE FROM migration.ledger WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName'")

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
                s"""INSERT INTO migration.ledger(
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
            throw RecomputeTilesException("Tile processing failed")
          }

          // Verify data integrity
          if (totalProcessed != originalCount) {
            logger.error(s"Data integrity check failed: Original count=$originalCount, Processed count=$totalProcessed")
            throw RecomputeTilesException("Data integrity check failed")
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
          throw RecomputeTilesException(s"Error during tile re-computation: $e")
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
      val primaryKeysDf = sourceScan().persist(cachingMode)
      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)

      val tiles = (0 until totalTiles).toList.par
      tiles.foreach(tile => {
        val rsTail = ledgerConnection.execute(getLedgerQueryBuilder(srcKeyspaceName, srcTableName, tile, "tail")).one()
        val rsHead = ledgerConnection.execute(getLedgerQueryBuilder(srcKeyspaceName, srcTableName, tile, "head")).one()

        val tail = Option(rsTail)
        val head = Option(rsHead)
        val tailLoadStatus = tail match {
          case t if t.isDefined => rsTail.getString("load_status")
          case _ => ""
        }
        val headLoadStatus = head match {
          case h if h.isDefined => rsHead.getString("load_status")
          case _ => ""
        }

        logger.info(s"Processing $tile, head is $head, tail is $tail, head status is $headLoadStatus, tail status is $tailLoadStatus")

        // Historical upload, the first round (head), no data in the ledger
        if (tail.isEmpty && head.isEmpty) {
          logger.info("Loading a head")
          val staged = primaryKeysDf.where(col("group") === tile)
          writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
          ledgerConnection.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
          val content = DiscoveryStats(tile, staged.count(), org.joda.time.LocalDateTime.now().toString)
          putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
        }

        // The second round (tail and head)
        if (!head.isEmpty && headLoadStatus == "SUCCESS") {
          // Tail never has been loaded
          if (tail.isEmpty) {
            logger.info("Loading a tail but keeping the head")
            val staged = primaryKeysDf.where(col("group") === tile)
            writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
            ledgerConnection.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
          }
        }

        // Swap tail and head
        if ((tail.isDefined && tailLoadStatus == "SUCCESS") && (head.isDefined && headLoadStatus == "SUCCESS")) {
          logger.info("Swapping the tail and the head")

          val staged = primaryKeysDf.where(col("group") === tile)
          writeWithSizeControl(staged, s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
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
              s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','');" +
              s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','');" +
              s"APPLY BATCH;"
          )
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
            measureTime("keysDiscoveryProcess", 60.0) {
              keysDiscoveryProcess()
            }

            if (workloadType.equals("batch")) {
              logger.info("The discovery job is completed")
              sys.exit()
            }
          }
          case "replication" => {
            measureTime("dataReplicationProcess", 120) {
              dataReplicationProcess()
            }

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