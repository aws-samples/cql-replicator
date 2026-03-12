/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

// Target Amazon DynamoDB - Preview

import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.retry.{PredefinedRetryPolicies, RetryPolicy}
import com.amazonaws.services.dynamodbv2.document.{Item, ItemUtils}
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils
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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import java.util.Base64
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import net.jpountz.xxhash.{XXHashFactory, XXHash64}

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
                             readTimeoutMS: Int = 120000, useCustomVarintReader: Boolean = false)
case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats
case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats
case class MaterializedViewConfig(enabled: Boolean = false, mvName: String = "")
case class PointInTimeReplicationConfig(predicateOp: String = "greaterThan")
case class DynamoDBPrimaryKey(partitionKeyName: String = "PK", partitionKeyType: String = "S", sortKeyName: String = "", sortKeyType: String = "S", separator: String = "#")
case class Replication(allColumns: Boolean = true, columns: List[String] = List(""), useCustomSerializer: Boolean = false, useMaterializedView: MaterializedViewConfig = MaterializedViewConfig(),
                       replicateWithTimestamp: Boolean = false,
                       pointInTimeReplicationConfig: PointInTimeReplicationConfig = PointInTimeReplicationConfig(),
                       readConfiguration: ReadConfiguration = ReadConfiguration(),
                       dynamoDBPrimaryKey: DynamoDBPrimaryKey = DynamoDBPrimaryKey())
case class CompressionConfig(enabled: Boolean = false, compressNonPrimaryColumns: List[String] = List(""), compressAllNonPrimaryKeysColumns: Boolean = false, targetAttributeName: String = "")
case class LargeObjectsConfig(enabled: Boolean = false, columns: List[String] = List(""), bucket: String = "", prefix: String = "", keySeparator: String = "#", compressionEnabled: Boolean = false)
case class TransformExpression(columnName: String, rule: String, alias: String = "", keepSource: Boolean = false)
case class Transformation(enabled: Boolean = false, addNonPrimaryKeyColumns: List[String] = List(), filterExpression: String = "",
                          transformExpressions: List[TransformExpression] = List())
case class UdtConversion(enabled: Boolean = true, columns: List[String] = List(""))
case class DDB(dynamoDBConnection: DynamoDBConnection, compressionConfig: CompressionConfig, largeObjectsConfig: LargeObjectsConfig, transformation: Transformation, readBeforeWrite: Boolean = false, udtConversion: UdtConversion, writeConfiguration: WriteConfiguration)
case class JsonMapping(replication: Replication, dynamodb: DDB)

case class DlqConfig(s3client: AmazonS3, bucketName: String, key: String)

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
        attributes.put(key, value) // Use put to maintain insertion order
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

  // Use it only for primary keys — respects DynamoDB key type (S or N) from configuration
  private def convertDDBValue(row: Row, position: Int, colName: String, colType: String): Map[String, AttributeValue] = {
    try {
      val stringValue: String = colType match {
        case t if StringTypes.contains(t) => row.getString(position)
        case "date" => row.getDate(position).toString
        case "timestamp" => handleTimestamp(row, position)
        case "time" => row.getLong(position).toString
        case t if NumericTypes.contains(t) => getNumericValue(row, position, t)
        case "uuid" | "timeuuid" => row.getString(position)
        case "boolean" => row.getBoolean(position).toString
        case "blob" => java.util.Base64.getEncoder.encodeToString(row.getAs[Array[Byte]](colName))
        case _ => throw CassandraTypeException(s"Unrecognized data type $colType")
      }
      // Always store as String — the actual DynamoDB attribute type (S or N) is applied
      // later in createPutRequest/executeDelete based on partitionKeyType/sortKeyType config
      Map(colName -> new AttributeValue().withS(stringValue))
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
      case "smallint" => row.get(position).asInstanceOf[Number].shortValue().toString
      case "tinyint" => row.get(position).asInstanceOf[Number].byteValue().toString
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

case class FlushingSet(flushingClient: AmazonDynamoDB, internalConfig: WriteConfiguration,
                       dlqConfig: DlqConfig, logger: GlueLogger, targetTable: String) {

  private val statements: ListBuffer[WriteRequest] = ListBuffer.empty

  final def add(element: WriteRequest): Unit = this.synchronized {
    if ( statements.size > internalConfig.maxStatementsPerBatch ) {
      flush()
      statements.append(element)
    } else {
      statements.append(element)
    }
  }

  def executeSinglePut(writeRequest: WriteRequest, readBeforeWrite: Boolean = false, attributeCondExpression: (String, Map[String, String])): Unit = {
    val putRequest = writeRequest.getPutRequest

    if (!readBeforeWrite) {
      val request = new PutItemRequest()
        .withTableName(targetTable)
        .withItem(putRequest.getItem)

      Try(flushingClient.putItem(request)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.error(s"Failed to execute single put after all attempts: ${e.getMessage}")
          persistToDlq(dlqConfig, ItemUtils.toItem(putRequest.getItem).toJSON)
      }
    }
    else {
      try {
        val request = new PutItemRequest()
          .withTableName(targetTable)
          .withItem(putRequest.getItem)
          .withConditionExpression(attributeCondExpression._1)
          .withExpressionAttributeNames(attributeCondExpression._2.asJava)

        Some(flushingClient.putItem(request))
      } catch {
        case _: ConditionalCheckFailedException =>
        case e: Exception =>
          logger.error(s"Failed to execute conditional put after all attempts: ${e.getMessage}")
          persistToDlq(dlqConfig, ItemUtils.toItem(putRequest.getItem).toJSON)
      }
    }
  }

  def executeSingleDelete(writeRequest: WriteRequest): Unit = {
    val deleteRequest = writeRequest.getDeleteRequest
    val request = new DeleteItemRequest()
      .withTableName(targetTable)
      .withKey(deleteRequest.getKey)
    Try(flushingClient.deleteItem(request)) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Failed to execute single delete after all attempts: ${e.getMessage}")
        persistToDlq(dlqConfig, ItemUtils.toItem(deleteRequest.getKey).toJSON)
    }
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

      if (!unprocessed.isEmpty) {
        logger.error(s"BatchWriteItem still has ${unprocessed.size()} unprocessed items after $retryCount retries, persisting to DLQ")
        unprocessed.values().asScala.foreach { items =>
          items.asScala.foreach { item =>
            val payload = Option(item.getPutRequest).map(pr => ItemUtils.toItem(pr.getItem).toJSON)
              .orElse(Option(item.getDeleteRequest).map(dr => ItemUtils.toItem(dr.getKey).toJSON))
              .getOrElse(item.toString)
            persistToDlq(dlqConfig, payload)
          }
        }
      }

      statements.clear()
    }
  }

  private def persistToDlq(dlqConfig: DlqConfig, payload: String): Unit = {
    val ts = org.joda.time.LocalDateTime.now().toString
    val key = s"${dlqConfig.key}/log-$ts.msg"

    try {
      dlqConfig.s3client.putObject(dlqConfig.bucketName, key, payload)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to persist to DLQ at $key: ${e.getMessage}")
    }
  }
}

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r


    def convertCassandraKeyToGenericKey(input: String, separator: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(2)}" }.mkString(separator)
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

    // Strictly Serializable - DynamoDB adapted replay
    def replayLogs(logger: GlueLogger,
                   ksName: String,
                   tblName: String,
                   bucketName: String,
                   s3Client: com.amazonaws.services.s3.AmazonS3,
                   tile: Int,
                   op: String,
                   ddbClient: AmazonDynamoDB,
                   targetTable: String): Unit = {
      Iterator.continually(isLogObjectPresent(ksName, tblName, bucketName, s3Client, tile, op)).takeWhile(_.nonEmpty).foreach {
        key => {
          val keyTmp = key.get
          val getObjectRequest = new GetObjectRequest(bucketName, keyTmp)
          val s3Object = s3Client.getObject(getObjectRequest)
          val objectContent = scala.io.Source.fromInputStream(s3Object.getObjectContent).mkString
          Try {
            logger.info(s"Detected a failed $op '$keyTmp' in the dlq. The $op is going to be replayed.")
            val attributeMap = InternalUtils.toAttributeValues(Item.fromJSON(objectContent))
            op match {
              case "insert" | "update" =>
                val request = new PutItemRequest().withTableName(targetTable).withItem(attributeMap)
                ddbClient.putItem(request)
              case "delete" =>
                val request = new DeleteItemRequest().withTableName(targetTable).withKey(attributeMap)
                ddbClient.deleteItem(request)
            }
          } match {
            case Failure(_) => throw new DlqS3Exception(s"Failed to replay $op: $objectContent")
            case Success(_) => {
              s3Client.deleteObject(bucketName, keyTmp)
              logger.info(s"Operation $op '$keyTmp' replayed and removed from the dlq successfully.")
            }
          }
        }
      }
    }

    def getDBConnection(connectionConfName: String, bucketName: String, s3client: AmazonS3): CqlSession = {
      val connectorConf = s3client.getObjectAsString(bucketName, s"artifacts/$connectionConfName")
      CqlSession.builder.withConfigLoader(DriverConfigLoader.fromString(connectorConf))
        .build()
    }

    def buildWritetimeExpression(columns: Seq[String]): String = columns match {
      case Seq(single) => s"writetime($single) as ts"
      case multiple    => multiple.map(c => s"writetime($c)").mkString("greatest(", ", ", ") as ts")
    }

    def inferKeys(cc: CqlSession, keyType: String, ks: String, tbl: String, columnTs: String, writetimeCols: Seq[String] = Seq.empty): Seq[mutable.LinkedHashMap[String, String]] = {
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
            mutable.LinkedHashMap(buildWritetimeExpression(writetimeCols) -> "bigint")

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

    // Let's do preflight checks
    logger.info("Preflight check started")
    preFlightCheck(internalConnectionToSource, srcKeyspaceName, srcTableName, "source", logger)
    preFlightCheck(internalConnectionToSource, trgKeyspaceName, trgTableName, "target", logger)
    logger.info("Preflight check completed")

    val pkFinal = columnTs match {
      case "None" => inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).map(_.keys.head)
      case _ => inferKeys(internalConnectionToSource, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs, writetimeColumns).map(_.keys.head)
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
    val pkFinalWithoutTs = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
    val clusteringColumns = pkFinalWithoutTs.filterNot(partitionKeys.contains)
    val pks = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
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

    val attrCondExpressionCAS = if (jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyName.isEmpty)
      ("attribute_not_exists(#pk)", Map("#pk" -> jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyName))
    else
      ("attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
        Map("#pk" -> jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyName,
          "#sk" -> jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyName))

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

    val ledgerTableName: String = "CQLReplicator.ledger"
    val ledgerClient: AmazonDynamoDB = getDDBConnection(
      jsonMapping4s.dynamodb.dynamoDBConnection.endpoint,
      jsonMapping4s.dynamodb.dynamoDBConnection.region
    )

    // --- DynamoDB Ledger helper functions ---

    def ledgerPartitionKey(ks: String, tbl: String, tile: Int): String =
      s"$ks#$tbl#$tile"

    def ensureLedgerTableExists(client: AmazonDynamoDB, tableName: String): Unit = {
      try {
        client.describeTable(tableName)
      } catch {
        case _: ResourceNotFoundException =>
          val request = new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(
              new KeySchemaElement("pk", KeyType.HASH),
              new KeySchemaElement("sk", KeyType.RANGE)
            )
            .withAttributeDefinitions(
              new AttributeDefinition("pk", ScalarAttributeType.S),
              new AttributeDefinition("sk", ScalarAttributeType.S)
            )
            .withBillingMode(BillingMode.PAY_PER_REQUEST)
          client.createTable(request)
          val waiter = new com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters(client)
          waiter.tableExists().run(
            new com.amazonaws.waiters.WaiterParameters(new DescribeTableRequest(tableName))
          )
      }
    }

    def getLedgerEntry(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String, tile: Int, ver: String): Option[Map[String, String]] = {
      val key = Map(
        "pk" -> new AttributeValue().withS(ledgerPartitionKey(ks, tbl, tile)),
        "sk" -> new AttributeValue().withS(ver)
      ).asJava
      val request = new GetItemRequest()
        .withTableName(tableName)
        .withKey(key)
      val result = client.getItem(request)
      Option(result.getItem).map { item =>
        val m = item.asScala.toMap
        Map(
          "offload_status" -> m.get("offload_status").map(_.getS).getOrElse(""),
          "dt_offload" -> m.get("dt_offload").map(_.getS).getOrElse(""),
          "location" -> m.get("location").map(_.getS).getOrElse(""),
          "load_status" -> m.get("load_status").map(_.getS).getOrElse(""),
          "dt_load" -> m.get("dt_load").map(_.getS).getOrElse("")
        )
      }
    }

    def deleteLedgerEntry(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String, tile: Int, ver: String): Unit = {
      val key = Map(
        "pk" -> new AttributeValue().withS(ledgerPartitionKey(ks, tbl, tile)),
        "sk" -> new AttributeValue().withS(ver)
      ).asJava
      val request = new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(key)
      client.deleteItem(request)
    }

    def deleteLedgerEntriesForTable(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String): Unit = {
      val prefix = s"$ks#$tbl#"
      var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null
      val allKeys = scala.collection.mutable.ListBuffer[(String, String)]()

      do {
        val scanRequest = new ScanRequest()
          .withTableName(tableName)
          .withFilterExpression("begins_with(pk, :prefix)")
          .withExpressionAttributeValues(Map(":prefix" -> new AttributeValue().withS(prefix)).asJava)
          .withProjectionExpression("pk, sk")
        if (lastEvaluatedKey != null) {
          scanRequest.withExclusiveStartKey(lastEvaluatedKey)
        }
        val result = client.scan(scanRequest)
        result.getItems.asScala.foreach { item =>
          allKeys += ((item.get("pk").getS, item.get("sk").getS))
        }
        lastEvaluatedKey = result.getLastEvaluatedKey
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

      allKeys.grouped(25).foreach { batch =>
        val deleteRequests = batch.map { case (pk, sk) =>
          new WriteRequest(new DeleteRequest(Map(
            "pk" -> new AttributeValue().withS(pk),
            "sk" -> new AttributeValue().withS(sk)
          ).asJava))
        }.asJava
        val batchRequest = new BatchWriteItemRequest()
          .withRequestItems(Map(tableName -> deleteRequests).asJava)
        var result = client.batchWriteItem(batchRequest)
        var unprocessed = result.getUnprocessedItems
        while (unprocessed != null && !unprocessed.isEmpty) {
          result = client.batchWriteItem(unprocessed)
          unprocessed = result.getUnprocessedItems
        }
      }
    }

    def getCurrentIcebergTilesCount(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String): Int = {
      val prefix = s"$ks#$tbl#"
      var lastEvaluatedKey: java.util.Map[String, AttributeValue] = null
      var count = 0

      do {
        val scanRequest = new ScanRequest()
          .withTableName(tableName)
          .withFilterExpression("begins_with(pk, :prefix) AND sk = :ver")
          .withExpressionAttributeValues(Map(
            ":prefix" -> new AttributeValue().withS(prefix),
            ":ver" -> new AttributeValue().withS("curr")
          ).asJava)
          .withSelect("COUNT")
        if (lastEvaluatedKey != null) {
          scanRequest.withExclusiveStartKey(lastEvaluatedKey)
        }
        val result = client.scan(scanRequest)
        count += result.getCount
        lastEvaluatedKey = result.getLastEvaluatedKey
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty)

      count
    }

    def recordDiscoverySnapshotDDB(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String, tile: Int, snapshotId: Long): Unit = {
      val pk = ledgerPartitionKey(ks, tbl, tile)
      val existingCurr = getLedgerEntry(client, tableName, ks, tbl, tile, "curr")
      val now = Instant.now().toString

      existingCurr match {
        case Some(currEntry) =>
          val prevItem = Map(
            "pk" -> new AttributeValue().withS(pk),
            "sk" -> new AttributeValue().withS("prev"),
            "offload_status" -> new AttributeValue().withS(currEntry.getOrElse("offload_status", "")),
            "dt_offload" -> new AttributeValue().withS(currEntry.getOrElse("dt_offload", "")),
            "location" -> new AttributeValue().withS(currEntry.getOrElse("location", "")),
            "load_status" -> new AttributeValue().withS(currEntry.getOrElse("load_status", "")),
            "dt_load" -> new AttributeValue().withS(currEntry.getOrElse("dt_load", ""))
          ).asJava

          val newCurrItem = Map(
            "pk" -> new AttributeValue().withS(pk),
            "sk" -> new AttributeValue().withS("curr"),
            "offload_status" -> new AttributeValue().withS("SUCCESS"),
            "dt_offload" -> new AttributeValue().withS(now),
            "location" -> new AttributeValue().withS(snapshotId.toString),
            "load_status" -> new AttributeValue().withS(""),
            "dt_load" -> new AttributeValue().withS("")
          ).asJava

          val transactRequest = new TransactWriteItemsRequest()
            .withTransactItems(
              new TransactWriteItem().withPut(new Put().withTableName(tableName).withItem(prevItem)),
              new TransactWriteItem().withPut(new Put().withTableName(tableName).withItem(newCurrItem))
            )
          client.transactWriteItems(transactRequest)

        case None =>
          val newCurrItem = Map(
            "pk" -> new AttributeValue().withS(pk),
            "sk" -> new AttributeValue().withS("curr"),
            "offload_status" -> new AttributeValue().withS("SUCCESS"),
            "dt_offload" -> new AttributeValue().withS(now),
            "location" -> new AttributeValue().withS(snapshotId.toString),
            "load_status" -> new AttributeValue().withS(""),
            "dt_load" -> new AttributeValue().withS("")
          ).asJava

          val putRequest = new PutItemRequest()
            .withTableName(tableName)
            .withItem(newCurrItem)
          client.putItem(putRequest)
      }
    }

    def markReplicationCompleteDDB(client: AmazonDynamoDB, tableName: String, ks: String, tbl: String, tile: Int): Unit = {
      val pk = ledgerPartitionKey(ks, tbl, tile)
      val now = Instant.now().toString

      val updateExpr = "SET load_status = :status, dt_load = :dt"
      val exprValues = Map(
        ":status" -> new AttributeValue().withS("SUCCESS"),
        ":dt" -> new AttributeValue().withS(now)
      ).asJava

      val updateCurr = new Update()
        .withTableName(tableName)
        .withKey(Map(
          "pk" -> new AttributeValue().withS(pk),
          "sk" -> new AttributeValue().withS("curr")
        ).asJava)
        .withUpdateExpression(updateExpr)
        .withExpressionAttributeValues(exprValues)

      val updatePrev = new Update()
        .withTableName(tableName)
        .withKey(Map(
          "pk" -> new AttributeValue().withS(pk),
          "sk" -> new AttributeValue().withS("prev")
        ).asJava)
        .withUpdateExpression(updateExpr)
        .withExpressionAttributeValues(exprValues)

      val transactRequest = new TransactWriteItemsRequest()
        .withTransactItems(
          new TransactWriteItem().withUpdate(updateCurr),
          new TransactWriteItem().withUpdate(updatePrev)
        )
      client.transactWriteItems(transactRequest)
    }

    def cleanupLedgerDDB(client: AmazonDynamoDB, tableName: String, logger: GlueLogger, ks: String, tbl: String, pt: String, cleanUpRequested: Boolean): Unit = {
      if (pt == "discovery" && cleanUpRequested) {
        logger.info(s"Cleaning up ledger entries for $ks.$tbl")
        deleteLedgerEntriesForTable(client, tableName, ks, tbl)
      }
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

    val writetimeExpr = buildWritetimeExpression(writetimeColumns)

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
          allColumnsFromSource.flatMap(_.keys) :+ writetimeExpr mkString ", "
        case s if !s.equals("None")
          && jsonMapping4s.replication.replicateWithTimestamp
          && !replicatedColumns.equals("*") =>
          s"$replicatedColumns, $writetimeExpr"
      }
    }

    // ---- Iceberg Table Management Functions (ported from Keyspaces version) ----

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
     * Uses null-safe comparison (eqNullSafe) for ts column to detect
     * null-to-value transitions correctly.
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
                  recordDiscoverySnapshotDDB(ledgerClient, ledgerTableName, ksName, tblName, tile, snapshotId)
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
    }

    // ---- End Iceberg Table Management Functions ----

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

    def compressValues(json: JValue): JValue = {
      if (jsonMapping4s.dynamodb.compressionConfig.enabled) {
        val jPayload = json

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

      val pkType = jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyType
      val skType = jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyType

      def toAttributeValue(rawValue: String, ddbKeyType: String): AttributeValue = {
        ddbKeyType.toUpperCase match {
          case "N" => new AttributeValue().withN(rawValue)
          case _ => new AttributeValue().withS(rawValue)
        }
      }

      // Handle partition key(s)
      val partitionKeyAttribute = if (partitionKeys.size == 1) {
        val key = partitionKeys.head
        val dataType = dataTypes.getOrElse(key,
          throw DynamoDBTypeException(s"Single partition key - use original type. Missing data type for partition key: $key"))
        (json \ key) match {
          case JNothing => throw DynamoDBTypeException(s"Single partition key - use original type. Missing partition key: $key")
          case JNull => throw DynamoDBTypeException(s"Single partition key - use original type. Partition key cannot be null: $key")
          case value =>
            val rawValue = createAttributeValue(value, dataType)
            // Apply the configured DynamoDB key type
            val strValue = Option(rawValue.getS).getOrElse(Option(rawValue.getN).getOrElse(value.extract[String]))
            toAttributeValue(strValue, pkType)
        }
      } else {
        val combinedKey = partitionKeys.map { key =>
          (json \ key) match {
            case JNothing => throw DynamoDBTypeException(s"Multiple partition keys - combine as String. Missing partition key: $key")
            case JNull => throw DynamoDBTypeException(s"Multiple partition keys - combine as String. Partition key cannot be null: $key")
            case value => value.extract[String]
          }
        }.mkString(separator)
        toAttributeValue(combinedKey, pkType)
      }

      // Handle sort key(s) if present
      val itemWithSortKey = if (sortKeys.nonEmpty) {
        val sortKeyAttribute = if (sortKeys.size == 1) {
          val key = sortKeys.head
          val dataType = dataTypes.getOrElse(key,
            throw DynamoDBTypeException(s"Handle sort key(s) if present. Missing data type for sort key: $key"))
          (json \ key) match {
            case JNothing => throw DynamoDBTypeException(s"Handle sort key(s) if present. Missing sort key: $key")
            case JNull => throw DynamoDBTypeException(s"Handle sort key(s) if present. Sort key cannot be null: $key")
            case value =>
              val rawValue = createAttributeValue(value, dataType)
              val strValue = Option(rawValue.getS).getOrElse(Option(rawValue.getN).getOrElse(value.extract[String]))
              toAttributeValue(strValue, skType)
          }
        } else {
          val combinedKey = sortKeys.map { key =>
            (json \ key) match {
              case JNothing => throw DynamoDBTypeException(s"Multiple sort keys - combine as String. Missing sort key: $key")
              case JNull => throw DynamoDBTypeException(s"Multiple sort keys - combine as String. Sort key cannot be null: $key")
              case value => value.extract[String]
            }
          }.mkString(separator)
          toAttributeValue(combinedKey, skType)
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
        lazy val dynamodbCnnParPar = getDDBConnection(jsonMapping4s.dynamodb.dynamoDBConnection.endpoint, jsonMapping4s.dynamodb.dynamoDBConnection.region)
        lazy val dlqConfig = DlqConfig(s3ClientOnPartition, bcktName, s"$srcKeyspaceName/$srcTableName/dlq/$tile/$op")
        lazy val fl = FlushingSet(dynamodbCnnParPar, jsonMapping4s.dynamodb.writeConfiguration, dlqConfig, logger, trgTableName)
        lazy val maxStatementsPerBatch = jsonMapping4s.dynamodb.writeConfiguration.maxStatementsPerBatch
        lazy val rowConverter = new RowConverter
        lazy val ddbPartitionKey = jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyName
        lazy val ddbSortKey = jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyName
        lazy val ddbKeySeparator = jsonMapping4s.replication.dynamoDBPrimaryKey.separator
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

          // Apply transform expressions to attributes for delete key computation
          val effectiveAttributes = if (jsonMapping4s.dynamodb.transformation.enabled && jsonMapping4s.dynamodb.transformation.transformExpressions.nonEmpty) {
            val transformed = mutable.LinkedHashMap[String, AttributeValue]() ++= attributes
            jsonMapping4s.dynamodb.transformation.transformExpressions.foreach { rule =>
              if (transformed.contains(rule.columnName)) {
                val originalValue = {
                  val av = transformed(rule.columnName)
                  Option(av.getS).orElse(Option(av.getN)).getOrElse("")
                }
                val hashedValue = hashValue(originalValue, rule.rule).toString
                val hashedAttr = rule.rule.toLowerCase match {
                  case "murmurhash3" => new AttributeValue().withN(hashedValue)
                  case "xxhash64" => new AttributeValue().withN(hashedValue)
                  case _ => new AttributeValue().withS(hashedValue)
                }
                if (rule.alias.nonEmpty) {
                  if (!rule.keepSource) {
                    transformed.remove(rule.columnName)
                  }
                  transformed.put(rule.alias, hashedAttr)
                } else {
                  transformed.put(rule.columnName, hashedAttr)
                }
              }
            }
            transformed
          } else {
            attributes
          }

          def combineKeys(keys: Seq[String]): String = {
            effectiveAttributes.filterKeys(keys.contains)
              .values
              .map(av =>
                Option(av.getS).orElse(Option(av.getN)).getOrElse("")
              )
              .mkString(ddbKeySeparator)
          }

          def toKeyAttribute(rawValue: String, ddbKeyType: String): AttributeValue = {
            ddbKeyType.toUpperCase match {
              case "N" => new AttributeValue().withN(rawValue)
              case _ => new AttributeValue().withS(rawValue)
            }
          }

          def toPkAttribute(value: AttributeValue): AttributeValue = {
            val raw = Option(value.getS).orElse(Option(value.getN)).getOrElse("")
            toKeyAttribute(raw, jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyType)
          }

          def toSkAttribute(value: AttributeValue): AttributeValue = {
            val raw = Option(value.getS).orElse(Option(value.getN)).getOrElse("")
            toKeyAttribute(raw, jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyType)
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
                fl.executeSingleDelete(createDeleteRequest(toPkAttribute(effectiveAttributes.values.head)))

              // Single partition key, single clustering column
              case (1, 1) =>
                fl.executeSingleDelete(createDeleteRequest(
                  toPkAttribute(effectiveAttributes(partitionKeys.head)),
                  Some(toSkAttribute(effectiveAttributes(clusteringColumns.head)))
                ))

              // Multiple partition keys and single clustering column
              case (n, 1) if n > 1 =>
                val partitionValue = toKeyAttribute(combineKeys(partitionKeys), jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyType)
                fl.executeSingleDelete(createDeleteRequest(
                  partitionValue,
                  Some(toSkAttribute(effectiveAttributes(clusteringColumns.head)))
                ))

              // Single partition key, multiple clustering columns
              case (1, n) if n > 1 =>
                val sortKeyValue = toKeyAttribute(combineKeys(clusteringColumns), jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyType)
                fl.executeSingleDelete(createDeleteRequest(
                  toPkAttribute(effectiveAttributes(partitionKeys.head)),
                  Some(sortKeyValue)
                ))

              // Multiple partition keys, no clustering columns
              case (_, 0) =>
                val partitionValue = toKeyAttribute(combineKeys(partitionKeys), jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyType)
                fl.executeSingleDelete(createDeleteRequest(partitionValue))

              // Multiple partition keys, multiple clustering columns
              case (_, n) if n > 1 =>
                val partitionValue = toKeyAttribute(combineKeys(partitionKeys), jsonMapping4s.replication.dynamoDBPrimaryKey.partitionKeyType)
                val sortKeyValue = toKeyAttribute(combineKeys(clusteringColumns), jsonMapping4s.replication.dynamoDBPrimaryKey.sortKeyType)
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
          val json4sRow = if (jsonMapping4s.dynamodb.transformation.enabled)
            valueTransformer(jsonRow, jsonMapping4s.dynamodb.transformation.transformExpressions)
          else
            jsonRow
          val backToJsonRow = backToCQLStatementWithoutTs(json4sRow)

          val ddbRequest = createPutRequest(backToJsonRow,
            partitionKeys.toList,
            clusteringColumns.toList,
            ddbAttributes,
            ddbKeySeparator,
            ddbPartitionKey,
            ddbSortKey
          )

          val isLargeObjects = jsonMapping4s.dynamodb.largeObjectsConfig.enabled
          val isReadBeforeWrite = jsonMapping4s.dynamodb.readBeforeWrite

          if (!isLargeObjects && !isReadBeforeWrite && maxStatementsPerBatch > 1) {
            // Batch processing for normal-sized objects
            fl.add(ddbRequest)
          } else if (isLargeObjects) {
            // Handle large objects - always single put + S3 offload
            fl.executeSinglePut(ddbRequest, isReadBeforeWrite, attrCondExpressionCAS)
            offloadToS3(backToJsonRow, s3ClientOnPartition, whereClause)
          } else {
            // Single put for either:
            // - maxStatementsPerBatch == 1 with normal-sized objects
            // - readBeforeWrite is true
            fl.executeSinglePut(ddbRequest, isReadBeforeWrite, attrCondExpressionCAS)
          }
        }

        partition.foreach(processRow)
        fl.flush()
        cassandraConnPerPar.close()
        dynamodbCnnParPar.shutdown()
      })
    }

    def shuffleDfV2(df: DataFrame): DataFrame = {
      val saltColumnName = java.util.UUID.randomUUID().toString
      val shuffledDf = df.withColumn(s"salt-$saltColumnName", rand())
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
      val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, currentTile)
      val pkColumnNames = pks

      logger.info(s"Starting replication for tile $currentTile, Iceberg table: $icebergTblName")

      // Read both curr and prev ledger entries from DynamoDB
      val currEntry = getLedgerEntry(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, currentTile, "curr")
      val prevEntry = getLedgerEntry(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, currentTile, "prev")

      logger.info(s"Ledger curr for tile $currentTile: ${currEntry.map(m => s"location=${m("location")}, load_status='${m("load_status")}', offload_status='${m("offload_status")}'").getOrElse("NOT FOUND")}")
      logger.info(s"Ledger prev for tile $currentTile: ${prevEntry.map(m => s"location=${m("location")}, load_status='${m("load_status")}', offload_status='${m("offload_status")}'").getOrElse("NOT FOUND")}")

      currEntry match {
        case None =>
          logger.info(s"No pending replication work for tile $currentTile")

        case Some(currMap) =>
          val currOffloadStatus = currMap.getOrElse("offload_status", "")
          val currLoadStatus = currMap.getOrElse("load_status", "")
          val currSnapshotIdOpt = Try(currMap("location").toLong).toOption

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
            val prevSnapshotIdOpt = prevEntry.flatMap(m => Try(m("location").toLong).toOption)
            val prevLoadStatus = prevEntry.map(m => m.getOrElse("load_status", "")).getOrElse("")

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
                        persist(insertsDf, columns, columnsPos, currentTile, "insert")
                        inserted = insertsDf.count()
                      }
                    }
                    case _ => {
                      val updatesDf = newUpdatesDF.drop("ts").persist(cachingMode)
                      if (!(insertsDf.isEmpty && updatesDf.isEmpty)) {
                        persist(insertsDf, columns, columnsPos, currentTile, "insert")
                        persist(updatesDf, columns, columnsPos, currentTile, "update")
                        inserted = insertsDf.count()
                        updated = updatesDf.count()
                      }
                      updatesDf.unpersist()
                    }
                  }

                  if (!deletesDf.isEmpty) {
                    persist(deletesDf, columns, columnsPos, currentTile, "delete")
                    deleted = deletesDf.count()
                  }

                  logger.info(s"Delta completed for tile $currentTile: inserts=$inserted, updates=$updated, deletes=$deleted")

                  if (updated != 0 || inserted != 0 || deleted != 0) {
                    val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, org.joda.time.LocalDateTime.now().toString)
                    putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
                  }

                  insertsDf.unpersist()
                  deletesDf.unpersist()

                  markReplicationCompleteDDB(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, currentTile)
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

                  persist(shuffleDfV2(sourceDfV2), columns, columnsPos, currentTile, "insert")
                  val cnt = sourceDfV2.count()
                  logger.info(s"Historical load completed for tile $currentTile: $cnt rows inserted")

                  val content = ReplicationStats(currentTile, cnt, 0, 0, 0, org.joda.time.LocalDateTime.now().toString)
                  putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)

                  markReplicationCompleteDDB(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, currentTile)
                  expireIcebergSnapshots(sparkSession, icebergTblName)
                }
            }
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

      val filterColumns = jsonMapping4s.dynamodb.transformation.addNonPrimaryKeyColumns match {
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
          if (!jsonMapping4s.dynamodb.transformation.enabled) df
          else if (jsonMapping4s.dynamodb.transformation.enabled && !jsonMapping4s.dynamodb.transformation.filterExpression.isEmpty)
            df.filter(jsonMapping4s.dynamodb.transformation.filterExpression)
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

    def recomputeTiles(): Unit = {
      try {
        // Use Iceberg-based tile count detection instead of S3 Parquet scan
        val currentNumTiles = getCurrentIcebergTilesCount(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName)
        logger.info(s"new tiles $totalTiles, current tiles $currentNumTiles")

        if (currentNumTiles > 0 && currentNumTiles != totalTiles) {
          logger.info(s"Detected a change in tiles from $currentNumTiles to $totalTiles. Recomputing tiles")

          // Read existing primary keys from per-tile Iceberg tables instead of Parquet
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

          // Infer PK columns using inferKeys (same as keysDiscoveryProcess)
          val pkColumnsForIceberg = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs)
            .map(m => { val (name, tpe) = m.head; Map("name" -> name, "type" -> tpe) })

          // Recompute tile assignment via xxhash64
          val hashColumns = pkFinalWithoutTs.map(col)
          val transformedDf = primaryKeys
            .withColumn("group", abs(xxhash64(concat(hashColumns: _*))) % totalTiles)
            .repartition(totalTiles, col("group"))
            .persist(cachingMode)

          // Clear all existing ledger entries for the source table before writing new entries
          deleteLedgerEntriesForTable(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName)

          // Create Iceberg database and ensure all tile tables exist sequentially
          // (same pattern as keysDiscoveryProcess to avoid Glue Catalog concurrency conflicts)
          val dbName = s"$icebergCatalog.${srcKeyspaceName}_db"
          sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
          for (tile <- 0 until totalTiles) {
            ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
          }

          // Write tiles in parallel via Iceberg + record snapshots in ledger
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

              // Record snapshot in ledger with both curr and prev entries using TransactWriteItems.
              // prev is marked as load_status='SUCCESS' so replication sees a valid baseline
              // and does a delta comparison (finding zero changes) instead of a full re-replicate.
              val snapshotIdOpt = currentIcebergSnapshotId(sparkSession, icebergTblName)
              snapshotIdOpt match {
                case Some(snapshotId) =>
                  val pk = ledgerPartitionKey(srcKeyspaceName, srcTableName, tile)
                  val now = Instant.now().toString

                  // Write prev entry with load_status=SUCCESS (baseline for delta comparison)
                  val prevItem = Map(
                    "pk" -> new AttributeValue().withS(pk),
                    "sk" -> new AttributeValue().withS("prev"),
                    "offload_status" -> new AttributeValue().withS("SUCCESS"),
                    "dt_offload" -> new AttributeValue().withS(now),
                    "location" -> new AttributeValue().withS(snapshotId.toString),
                    "load_status" -> new AttributeValue().withS("SUCCESS"),
                    "dt_load" -> new AttributeValue().withS(now)
                  ).asJava

                  // Write curr entry with load_status empty (so replication will process it)
                  val currItem = Map(
                    "pk" -> new AttributeValue().withS(pk),
                    "sk" -> new AttributeValue().withS("curr"),
                    "offload_status" -> new AttributeValue().withS("SUCCESS"),
                    "dt_offload" -> new AttributeValue().withS(now),
                    "location" -> new AttributeValue().withS(snapshotId.toString),
                    "load_status" -> new AttributeValue().withS(""),
                    "dt_load" -> new AttributeValue().withS("")
                  ).asJava

                  val transactRequest = new TransactWriteItemsRequest()
                    .withTransactItems(
                      new TransactWriteItem().withPut(new Put().withTableName(ledgerTableName).withItem(prevItem)),
                      new TransactWriteItem().withPut(new Put().withTableName(ledgerTableName).withItem(currItem))
                    )
                  ledgerClient.transactWriteItems(transactRequest)
                  logger.info(s"Recorded snapshot $snapshotId for tile $tile (curr + prev)")
                case None =>
                  logger.warn(s"Could not retrieve snapshot ID after writing tile $tile")
              }

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

          // Validate results — row count integrity check
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

          // Clean up orphaned Iceberg tiles when tile count decreases
          if (totalTiles < currentNumTiles) {
            for (tile <- totalTiles until currentNumTiles) {
              dropIcebergTileTable(sparkSession, srcKeyspaceName, srcTableName, tile)
              Try {
                deleteLedgerEntry(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, tile, "curr")
                deleteLedgerEntry(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, tile, "prev")
              } match {
                case Failure(e) => logger.warn(s"Failed to delete ledger entries for orphaned tile $tile: ${e.getMessage}")
                case Success(_) => logger.info(s"Deleted ledger entries for orphaned tile $tile")
              }
            }
          }

          // Unpersist cached DataFrames after completion
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
        val currEntry = getLedgerEntry(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, tile, "curr")
        val currLoadStatus = currEntry.map(_.getOrElse("load_status", "")).getOrElse("")

        if (currEntry.isDefined && currLoadStatus != "SUCCESS") {
          logger.info(s"Skipping tile $tile: replication has not yet consumed the current snapshot (load_status='$currLoadStatus')")
        } else {
          val tileDf = groupedPkDF.filter(col("tile") === tile).drop("tile")
          writeIcebergTileSnapshot(sparkSession, tileDf, icebergTblName)
          val snapshotIdOpt = currentIcebergSnapshotId(sparkSession, icebergTblName)
          snapshotIdOpt match {
            case Some(snapshotId) =>
              recordDiscoverySnapshotDDB(ledgerClient, ledgerTableName, srcKeyspaceName, srcTableName, tile, snapshotId)
              logger.info(s"Recorded snapshot $snapshotId for tile $tile")
            case None =>
              logger.warn(s"Could not retrieve snapshot ID after writing tile $tile")
          }
          val tileCount = groupedPkDF.where(col("tile") === tile).count()
          val content = DiscoveryStats(tile, tileCount, org.joda.time.LocalDateTime.now().toString)
          putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
        }
      })
      groupedPkDF.unpersist()
      primaryKeysDf.unpersist()
    }

    // Initialize DynamoDB ledger table (auto-creates if it doesn't exist)
    ensureLedgerTableExists(ledgerClient, ledgerTableName)

    cleanupLedgerDDB(ledgerClient, ledgerTableName, logger, srcKeyspaceName, srcTableName, processType, cleanUpRequested)

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
            measureTime("keysDiscoveryProcess", 60.0) {
              keysDiscoveryProcess()
            }

            if (workloadType.equals("batch")) {
              logger.info("The discovery job is completed")
              sys.exit()
            }
          }
          case "replication" => {
            if (replayLog) {
              val replayDdbClient = getDDBConnection(jsonMapping4s.dynamodb.dynamoDBConnection.endpoint,
                jsonMapping4s.dynamodb.dynamoDBConnection.region)
              try {
                // Replay inserts
                replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "insert", replayDdbClient, trgTableName)
                // Replay updates
                replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "update", replayDdbClient, trgTableName)
                // Replay deletes
                replayLogs(logger, srcKeyspaceName, srcTableName, bcktName, s3client, currentTile, "delete", replayDdbClient, trgTableName)
              } finally {
                replayDdbClient.shutdown()
              }
            }
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
    ledgerClient.shutdown()
    Job.commit()
  }
}
