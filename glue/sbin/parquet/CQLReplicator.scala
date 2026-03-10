/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon S3 in parquet format

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, GetObjectRequest, ListObjectsV2Request, ObjectListing, ObjectMetadata}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.spark.connector.cql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.joda.time.LocalDateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.util.Base64
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.ParSet
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
class ProcessTypeException(s: String) extends RuntimeException {
  println(s)
}
class CassandraTypeException(s: String) extends RuntimeException {
  println(s)
}
class StatsS3Exception(s: String) extends RuntimeException {
  println(s)
}
class PersistToTargetException(s: String) extends RuntimeException {
  println(s)
}

class PreFlightCheckException(val message: String, val errorCode: Int, val cause: Throwable = null)
  extends Exception(s"[$errorCode] $message", cause) {
  def this(message: String) = this(message, 0)

  def this(message: String, cause: Throwable) = this(message, 0, cause)

  override def toString: String = s"PreFlightCheckException($errorCode, $message)"
}

case class MaterialzedViewConfig(enabled: Boolean = false, mvName: String = "")
case class TtlAddOn(enabled: Boolean = false, predicateOp: String = "greaterThan", predicateVal: Long = 0)
case class PointInTimeReplicationConfig(predicateOp: String = "greaterThan")
case class Replication(allColumns: Boolean = true,
                       ttlAddOn: TtlAddOn = TtlAddOn(),
                       pointInTimeReplicationConfig: PointInTimeReplicationConfig = PointInTimeReplicationConfig(),
                       columns: List[String] = List(""),
                       useCustomSerializer: Boolean = false,
                       useMaterializedView: MaterialzedViewConfig = MaterialzedViewConfig())
case class S3Config(bucket: String = null, prefix: String = null, maxFileSizeMb: Int = 32)
case class JsonMapping(replication: Replication, s3: S3Config)

sealed trait Stats

case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats

case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats

class CustomSerializationException(s: String) extends RuntimeException {
  println(s)
}

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

class SupportFunctions {
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

case class FlushingSet[T](maxSize: Int, flushingClient: com.amazonaws.services.s3.AmazonS3, bucketName: String, key: String) {
  private var set: ParSet[T] = ParSet.empty
  private var size: Int = 0

  final def add(element: T): Unit = this.synchronized {
    val elementSize = element.toString.getBytes.length
    if (size + elementSize >= maxSize) {
      flush()
      size = elementSize
      set = ParSet.empty + element
    } else {
      size += elementSize
      set = set + element
    }
  }

  def flush(): Unit = this.synchronized {
    if (set.nonEmpty) {
      val fileName = Uuids.timeBased
      val data = set.mkString("\n")
      val inputStream = new ByteArrayInputStream(data.getBytes)
      val metadata = new ObjectMetadata()
      metadata.setContentLength(data.length)
      flushingClient.putObject(bucketName, s"$key/$fileName.json", inputStream, metadata)
      set = ParSet.empty
      size = 0
    }
  }

  def getSize: Int = this.synchronized {
    size
  }
}

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {

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
          ReplicationStats(0, 0, 0, 0, 0, LocalDateTime.now().toString)
        }
        case Success(json) => {
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(json).extract[ReplicationStats]
        }
      }
    }

    def shuffleDfV2(df: DataFrame): DataFrame = {
      df.orderBy(rand())
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
        case Some("None") => JsonMapping(Replication(), S3Config())
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
    val sparkConf: SparkConf = sparkContext.getConf
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
    val workloadTypeArg = Try(args("WORKLOAD_TYPE")).getOrElse("continuous")
    val workloadType = if (workloadTypeArg == "continuous" || workloadTypeArg == "batch") workloadTypeArg
    else {
      logger.error(s"ERROR: Invalid workload type '$workloadTypeArg'. Supported values are 'continuous' and 'batch'")
      sys.exit()
    }
    // Internal configuration+
    val WAIT_TIME = safeMode match {
      case "true" => 25000
      case _ => 0
    }
    val cachingMode = safeMode match {
      case "true" => StorageLevel.DISK_ONLY
      case _ => StorageLevel.MEMORY_AND_DISK_SER
    }

    val processType = args("PROCESS_TYPE") // discovery or replication
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r
    val patternWhereClauseToMap: Regex = """(\w+)=['"]?(.*?)['"]?(?: and |$)""".r
    sparkSession.conf.set(s"spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")

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
    val defaultPartitions = scala.math.max(2, (sparkContext.defaultParallelism / 2 - 2))
    val cleanUpRequested: Boolean = args("CLEANUP_REQUESTED") match {
      case "false" => false
      case _ => true
    }

    //AmazonS3Client to check if a stop request was issued
    val s3ClientConf = new ClientConfiguration().withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    val s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(s3ClientConf).build()

    val internalConnectionToSource = getDBConnection("CassandraConnector.conf", bcktName, s3client)
    val internalConnectionToTarget = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
    // Retain CassandraConnector for spark-cassandra-connector reads and persistToTarget partition-level access
    val cassandraConn = CassandraConnector(sparkContext.getConf.set("spark.cassandra.connection.config.profile.path", "CassandraConnector.conf"))

    // Let's do preflight checks
    logger.info("Preflight check started")
    preFlightCheck(internalConnectionToSource, srcKeyspaceName, srcTableName, "source", logger)
    logger.info("Preflight check completed")

    val pkFinal = columnTs match {
      case "None" => inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
      case _ => inferKeys(internalConnectionToSource, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs, writetimeColumns).flatten.toMap.keys.toSeq
    }

    val pkFinalWithoutTs = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
    val pks = if (writetimeColumns.nonEmpty) pkFinal.filterNot(_ == buildWritetimeExpression(writetimeColumns)) else pkFinal
    val columns = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex
    val allColumnsFromSource = getAllColumns(internalConnectionToSource, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList
    val jsonMappingRaw = new String(Base64.getDecoder.decode(jsonMapping.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    logger.info(s"Json mapping: $jsonMappingRaw")
    val jsonMapping4s = parseJSONMapping(jsonMappingRaw.replaceAll("\\r\\n|\\r|\\n", ""))
    val replicatedColumns = jsonMapping4s match {
      case JsonMapping(Replication(true, _, _, _, _, _), _) => "*"
      case rep => rep.replication.columns.mkString(",")
    }

    val maxFileSizeMb = Option(jsonMapping4s.s3.maxFileSizeMb).getOrElse(32)
    val flushingThreshold = 1024 * 1024 * maxFileSizeMb

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

    def convertToMap(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"'${m.group(1)}':'${m.group(2)}'" }.mkString(", ")
    }

    def stopRequested(bucket: String): Boolean = {
      if (processType == "resize") return false
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

    def cleanUpJsonObjects(bucket: String, key: String): Unit = {
      @tailrec
      def deleteObjects(objectListing: ObjectListing): Unit = {
        val summaries = objectListing.getObjectSummaries.asScala.toList

        if (summaries.nonEmpty) {
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

    def getSourceRow(cls: String, wc: String, session: CqlSession, defaultFormat: org.json4s.Formats): String = {
      val rs = if (jsonMapping4s.replication.useCustomSerializer) {
        val row = Option(session.execute(s"SELECT $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc").one())
        if (row.nonEmpty)
          Serialization.write(row)(defaultFormat)
        else
          ""
      } else {
        val row = Option(session.execute(s"SELECT json $cls FROM $srcKeyspaceName.$srcTableName WHERE $wc").one())
        if (row.nonEmpty)
          row.get.getString(0).replace("'", "\\\\u0027")
        else
          ""
      }
      rs
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String],
                        columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      df.rdd.foreachPartition(
        partition => {
          val customFormat = jsonMapping4s.replication.useCustomSerializer match {
            case true => DefaultFormats + new CustomResultSetSerializer
            case _ => DefaultFormats
          }
          val supportFunctions = new SupportFunctions()
          val s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3 = AmazonS3ClientBuilder.defaultClient()
          val fl = FlushingSet[String](flushingThreshold, s3ClientOnPartition, bcktName, s"tmp/$trgKeyspaceName/$trgTableName/$tile/$op")
          partition.foreach(
            row => {
              val whereClause = rowToStatement(row, columns, columnsPos)
              if (whereClause.nonEmpty) {
                cassandraConn.withSessionDo { session => {
                  if (op == "insert" || op == "update") {
                    val rs = ttlColumn match {
                      case "None" => getSourceRow(replicatedColumns, whereClause, session, customFormat)
                      case _ => getSourceRow(selectStmtWithTTL, whereClause, session, customFormat)
                    }

                    rs.isEmpty match {
                      case true => logger.info(s"$whereClause not found in the source, the row might be already deleted or expired")
                      case false if ttlColumn == "None" => {
                        val jsonRow = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs)
                        fl.add(jsonRow)
                      }
                      case false if ttlColumn != "None" => {
                        if (jsonMapping4s.replication.ttlAddOn.enabled) {
                          val jsonRow = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs)
                          val json4sRow = parse(jsonRow)
                          val ttlVal = getTTLvalue(json4sRow)
                          val ttlValConst = jsonMapping4s.replication.ttlAddOn.predicateVal
                          jsonMapping4s.replication.ttlAddOn.predicateOp match {
                            case "equal" if ttlVal == ttlValConst => fl.add(jsonRow)
                            case "greaterThan" if ttlVal > ttlValConst => fl.add(jsonRow)
                            case "lessThan" if ttlVal < ttlValConst => fl.add(jsonRow)
                            case _ =>
                          }
                        }
                      }
                    }
                  }
                  if (op == "delete") {
                    val keyValuePairs = convertToMap(whereClause)
                    fl.add(s"{$keyValuePairs}")
                  }
                }
                }
              }
            }
          )
          fl.getSize match {
            case s if s > 0 => fl.flush()
            case _ =>
          }
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
          case "short" => row.getInt(position)
          case "decimal" => row.getDecimal(position)
          case "tinyint" => row.getInt(position)
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

    def s3prefixExist(s3Client: com.amazonaws.services.s3.AmazonS3, bucketName: String, prefix: String): Boolean = {
      val request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix)
      val result = s3Client.listObjectsV2(request)
      result.getKeyCount > 0
    }

    def jsonToParquet(df: DataFrame, op: String, tile: Int): Long = {
      val cnt = df.isEmpty match {
        case false => {
          persistToTarget(shuffleDfV2(df.drop("ts", "group")), columns, columnsPos, tile, op)
          val prefix = s"tmp/$trgKeyspaceName/$trgTableName/$tile/$op"
          val isS3NonEmpty = s3prefixExist(s3client, bcktName, prefix)
          if (isS3NonEmpty == true) {
            val fingerPrint = LocalDateTime.now()
            val dfFromJson = sparkSession.read.option("inferSchema", "true").json(s"$landingZone/tmp/$trgKeyspaceName/$trgTableName/$tile/$op/*").dropDuplicates()
            val parquetBucket = Option(jsonMapping4s.s3.bucket).getOrElse(bcktName)
            val parquetPrefix = Option(jsonMapping4s.s3.prefix).getOrElse("parquet-storage")
            dfFromJson.write.format("parquet")
              .mode("overwrite")
              .save(s"s3://$parquetBucket/$parquetPrefix/$trgKeyspaceName/$trgTableName/$tile/$op/$fingerPrint")
          }
          cleanUpJsonObjects(bcktName, s"tmp/$trgKeyspaceName/$trgTableName/$tile/$op")
        }
          df.count()
        case _ => 0
      }
      cnt
    }

    /**
     * Builds a DataFrame reading from the source Cassandra table via spark-cassandra-connector.
     * Reads from the materialized view if configured, otherwise from the base table.
     * Applies column selection based on replicatedColumns configuration.
     *
     * @return DataFrame with selected columns from the source table
     */
    def buildSourceTableDf(): DataFrame = {
      val sourceTable = if (jsonMapping4s.replication.useMaterializedView.enabled) {
        jsonMapping4s.replication.useMaterializedView.mvName
      } else {
        srcTableName
      }

      val baseDf = sparkSession.read.cassandraFormat(sourceTable, srcKeyspaceName)
        .option("inferSchema", "true")
        .load()

      if (replicatedColumns == "*") {
        baseDf
      } else {
        val selectedCols = replicatedColumns.split(",").map(_.trim)
        val pkCols = pks
        val allCols = (pkCols ++ selectedCols).distinct
        baseDf.select(allCols.map(col): _*)
      }
    }

    /**
     * Estimates the output size and coalesces/repartitions to target file size.
     * Uses row count and average row size estimation based on the DataFrame schema.
     *
     * @param df             DataFrame to coalesce
     * @param targetSizeMb   Target file size in MB
     * @return               Coalesced/repartitioned DataFrame
     */
    def estimateAndCoalesce(df: DataFrame, targetSizeMb: Int): DataFrame = {
      val rowCount = df.count()
      if (rowCount == 0) return df

      val avgRowSizeBytes = df.schema.fields.map { field =>
        field.dataType match {
          case org.apache.spark.sql.types.StringType    => 64
          case org.apache.spark.sql.types.BinaryType    => 128
          case org.apache.spark.sql.types.IntegerType   => 4
          case org.apache.spark.sql.types.LongType      => 8
          case org.apache.spark.sql.types.FloatType     => 4
          case org.apache.spark.sql.types.DoubleType    => 8
          case org.apache.spark.sql.types.BooleanType   => 1
          case org.apache.spark.sql.types.ShortType     => 2
          case org.apache.spark.sql.types.ByteType      => 1
          case _: org.apache.spark.sql.types.DecimalType => 16
          case org.apache.spark.sql.types.DateType      => 4
          case org.apache.spark.sql.types.TimestampType => 8
          case _                                        => 32
        }
      }.sum + 8 // 8 bytes overhead per row

      val totalSizeBytes = rowCount * avgRowSizeBytes
      val targetSizeBytes = targetSizeMb.toLong * 1024L * 1024L
      val targetPartitions = math.max(1, math.ceil(totalSizeBytes.toDouble / targetSizeBytes).toInt)
      val currentPartitions = df.rdd.getNumPartitions

      if (targetPartitions < currentPartitions) {
        df.coalesce(targetPartitions)
      } else if (targetPartitions > currentPartitions) {
        df.repartition(targetPartitions)
      } else {
        df
      }
    }

    /**
     * Retrieves TTL values for the given primary keys using CQL queries.
     * Uses mapPartitions with cassandraConn to execute targeted TTL lookups.
     * Returns a DataFrame with PK columns and a ttl_value column.
     *
     * @param pksDf  DataFrame of primary keys to look up TTL for
     * @return       DataFrame with PK columns and ttl_value column
     */
    def retrieveTtlValues(pksDf: DataFrame): DataFrame = {
      val pkSchema = pksDf.schema
      val ttlCol = ttlColumn
      val srcKs = srcKeyspaceName
      val srcTbl = srcTableName
      val pkCols = columns
      val pkColsPos = columnsPos

      val ttlOutputSchema = pkSchema.add("ttl_value", org.apache.spark.sql.types.LongType, nullable = false)

      val ttlRdd = pksDf.rdd.mapPartitions { partition =>
        cassandraConn.withSessionDo { session =>
          partition.map { row =>
            val whereClause = rowToStatement(row, pkCols, pkColsPos)
            val ttlVal = if (whereClause.nonEmpty) {
              Try {
                val rs = session.execute(
                  s"SELECT ttl($ttlCol) FROM $srcKs.$srcTbl WHERE $whereClause"
                )
                val resultRow = Option(rs.one())
                resultRow match {
                  case Some(r) if !r.isNull(0) => r.getInt(0).toLong
                  case _ => 0L
                }
              } match {
                case Success(v) => v
                case Failure(e) =>
                  logger.warn(s"Failed to retrieve TTL for $whereClause: ${e.getMessage}")
                  0L
              }
            } else {
              0L
            }
            Row.fromSeq(row.toSeq :+ ttlVal)
          }
        }
      }

      sparkSession.createDataFrame(ttlRdd, ttlOutputSchema)
    }

    /**
     * Bulk replication: joins changed-PKs DataFrame with source table read,
     * applies column selection and optional TTL filtering, writes Parquet directly to S3.
     *
     * @param changedPksDf  DataFrame of changed primary keys (from computeIcebergChanges)
     * @param op            Operation type: "insert", "update", or "delete"
     * @param tile          Current tile number
     * @return              Number of rows written
     */
    def bulkReplicateToParquet(changedPksDf: DataFrame, op: String, tile: Int): Long = {
      if (changedPksDf.isEmpty) return 0L

      val fingerPrint = LocalDateTime.now()
      val parquetBucket = Option(jsonMapping4s.s3.bucket).getOrElse(bcktName)
      val parquetPrefix = Option(jsonMapping4s.s3.prefix).getOrElse("parquet-storage")
      val outputPath = s"s3://$parquetBucket/$parquetPrefix/$trgKeyspaceName/$trgTableName/$tile/$op/$fingerPrint"
      val bulkTargetSizeMb = Option(jsonMapping4s.s3.maxFileSizeMb).filter(_ > 0).getOrElse(32)

      op match {
        case "insert" | "update" =>
          val pkOnlyDf = changedPksDf.drop("ts", "group")
          val sourceDf = buildSourceTableDf()

          // Cast PK columns from the changed-PKs DataFrame to match the source table's types
          // to avoid type mismatches between Iceberg (e.g., STRING for uuid) and spark-cassandra-connector
          val sourceSchema = sourceDf.schema
          val castPkDf = pks.foldLeft(pkOnlyDf) { (df, pkCol) =>
            sourceSchema.find(_.name == pkCol) match {
              case Some(field) => df.withColumn(pkCol, col(pkCol).cast(field.dataType))
              case None => df
            }
          }

          val joinedDf = sourceDf.join(broadcast(castPkDf), pks, "inner")

          val resultDf = if (jsonMapping4s.replication.ttlAddOn.enabled) {
            val ttlDf = retrieveTtlValues(pkOnlyDf)
            val withTtl = joinedDf.join(ttlDf, pks, "inner")
            val ttlValConst = jsonMapping4s.replication.ttlAddOn.predicateVal
            val filtered = jsonMapping4s.replication.ttlAddOn.predicateOp match {
              case "greaterThan" => withTtl.filter(col("ttl_value") > ttlValConst)
              case "lessThan"    => withTtl.filter(col("ttl_value") < ttlValConst)
              case "equal"       => withTtl.filter(col("ttl_value") === ttlValConst)
              case _             => withTtl.filter(col("ttl_value") > ttlValConst)
            }
            filtered.drop("ttl_value")
          } else {
            joinedDf
          }

          val cachedDf = resultDf.persist(cachingMode)
          val rowCount = cachedDf.count()
          logger.info(s"Bulk $op for tile $tile: pkOnlyDf=${pkOnlyDf.count()}, sourceDf schema=${sourceDf.schema.simpleString}, joinedDf=$rowCount rows, outputPath=$outputPath")
          if (rowCount > 0) {
            val coalescedDf = estimateAndCoalesce(cachedDf, bulkTargetSizeMb)
            coalescedDf.write.format("parquet").mode("overwrite").save(outputPath)
          }
          cachedDf.unpersist()
          rowCount

        case "delete" =>
          val pkOnlyDf = changedPksDf.drop("ts", "group")
          val cachedDf = pkOnlyDf.persist(cachingMode)
          val rowCount = cachedDf.count()
          logger.info(s"Bulk delete for tile $tile: $rowCount rows, outputPath=$outputPath")
          if (rowCount > 0) {
            val coalescedDf = estimateAndCoalesce(cachedDf, bulkTargetSizeMb)
            coalescedDf.write.format("parquet").mode("overwrite").save(outputPath)
          }
          cachedDf.unpersist()
          rowCount

        case _ => 0L
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
     * Records a new Iceberg snapshot in the ledger for a tile.
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
        s"SELECT location, load_status FROM migration.ledger WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='curr'"
      ).one())

      existingCurr match {
        case Some(row) =>
          val oldSnapshotId = row.getString("location")
          val oldLoadStatus = Option(row.getString("load_status")).getOrElse("")
          session.execute(
            s"BEGIN UNLOGGED BATCH " +
              s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'$oldSnapshotId','prev','$oldLoadStatus',toTimestamp(now()));" +
              s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','');" +
              s"APPLY BATCH;"
          )
        case None =>
          session.execute(
            s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','')"
          )
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
        s"SELECT location FROM migration.ledger WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='prev'"
      ).one())

      val curr = Option(session.execute(
        s"SELECT location FROM migration.ledger WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='curr'"
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
          s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'prev','SUCCESS',toTimestamp(now()));" +
          s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'curr','SUCCESS',toTimestamp(now()));" +
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

    // ======================== Tile Recomputation Functions ========================

    /**
     * Counts the current number of tiles from ledger 'curr' entries.
     * Uses ALLOW FILTERING to count distinct tiles for this table.
     */
    def getCurrentIcebergTilesCount(ledgerSession: CqlSession): Int = {
      val rs = ledgerSession.execute(
        s"SELECT tile FROM migration.ledger " +
          s"WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName' AND ver='curr' ALLOW FILTERING"
      )
      rs.all().size()
    }

    /**
     * Drops a per-tile Iceberg table via DROP TABLE IF EXISTS.
     * Logs and swallows errors to avoid aborting the resize cycle.
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
     * Recomputes tiles when the tile count changes.
     * Reads PKs from existing Iceberg tables, redistributes across the new tile count,
     * writes new Iceberg snapshots, validates integrity, and drops orphaned tiles.
     */
    def recomputeTiles(): Unit = {
      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        val currentNumTiles = getCurrentIcebergTilesCount(ledgerConnection)
        logger.info(s"new tiles $totalTiles, current tiles $currentNumTiles")

        if (currentNumTiles > 0 && currentNumTiles != totalTiles) {
          logger.info(s"Detected a change in tiles from $currentNumTiles to $totalTiles. Recomputing tiles")

          // 1. Read all PKs from existing Iceberg tables
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

          // 2. Infer PK columns for Iceberg schema (same as keysDiscoveryProcess)
          val pkColumnsForIceberg = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs)
            .map(m => { val (name, tpe) = m.head; Map("name" -> name, "type" -> tpe) })

          // 3. Redistribute with xxhash64 grouping
          val hashColumns = pkFinalWithoutTs.map(col)
          val transformedDf = primaryKeys
            .withColumn("group", abs(xxhash64(concat(hashColumns: _*))) % totalTiles)
            .repartition(totalTiles, col("group"))
            .persist(cachingMode)

          // 4. Clear existing ledger entries
          ledgerConnection.execute(s"DELETE FROM migration.ledger WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName'")

          // 5. Create Iceberg database and tables sequentially
          val dbName = s"$icebergCatalog.${srcKeyspaceName}_db"
          sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
          for (tile <- 0 until totalTiles) {
            ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
          }

          // 6. Write tiles in parallel, record snapshots with curr+prev
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
                      s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','prev','SUCCESS',toTimestamp(now()));" +
                      s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','');" +
                      s"APPLY BATCH;"
                  )
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

          // 7. Validate row count integrity
          val (successes, failures) = results.partition(_.isRight)
          val totalProcessed = successes.collect { case Right((_, count)) => count }.sum

          if (failures.nonEmpty) {
            logger.error(s"Failed to process ${failures.size} tiles")
            failures.foreach {
              case Left((tile, error)) =>
                logger.error(s"Tile $tile failed: ${error.getMessage}")
              case _ =>
            }
            throw new RuntimeException("Tile processing failed")
          }

          if (totalProcessed != originalCount) {
            logger.error(s"Data integrity check failed: Original count=$originalCount, Processed count=$totalProcessed")
            throw new RuntimeException("Data integrity check failed")
          }

          // 8. Drop orphaned tiles if count decreased
          if (totalTiles < currentNumTiles) {
            for (tile <- totalTiles until currentNumTiles) {
              dropIcebergTileTable(sparkSession, srcKeyspaceName, srcTableName, tile)
              Try {
                ledgerConnection.execute(
                  s"DELETE FROM migration.ledger WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName' AND tile=$tile"
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

    def dataReplicationProcess(): Unit = {
      val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, currentTile)
      val pkColumnNames = pkFinalWithoutTs

      val session = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        logger.info(s"Starting replication for tile $currentTile, Iceberg table: $icebergTblName")

        // Read both curr and prev ledger entries
        val currEntry = Option(session.execute(
          s"SELECT location, load_status, offload_status FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and ver='curr'"
        ).one())

        val prevEntry = Option(session.execute(
          s"SELECT location, load_status, offload_status FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and ver='prev'"
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
              logger.info(s"Current snapshot already replicated for tile $currentTile")
            } else {
              val currSnapshotId = currSnapshotIdOpt.get

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

                    val (newInsertsDF, newDeletesDF, newUpdatesDF) = computeIcebergChanges(
                      sparkSession, icebergTblName, prevSnapshotId, currSnapshotId,
                      pkColumnNames, columnTs
                    )

                    columnTs match {
                      case "None" => {
                        val (inserted, deleted) = if (jsonMapping4s.replication.useCustomSerializer) {
                          (jsonToParquet(newInsertsDF, "insert", currentTile),
                           jsonToParquet(newDeletesDF, "delete", currentTile))
                        } else {
                          (bulkReplicateToParquet(newInsertsDF, "insert", currentTile),
                           bulkReplicateToParquet(newDeletesDF, "delete", currentTile))
                        }
                        logger.info(s"Delta completed for tile $currentTile: inserts=$inserted, deletes=$deleted")
                        val content = ReplicationStats(currentTile, 0, 0, inserted, deleted, LocalDateTime.now().toString)
                        putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
                      }
                      case _ => {
                        val (inserted, updated, deleted) = if (jsonMapping4s.replication.useCustomSerializer) {
                          (jsonToParquet(newInsertsDF, "insert", currentTile),
                           jsonToParquet(newUpdatesDF, "update", currentTile),
                           jsonToParquet(newDeletesDF, "delete", currentTile))
                        } else {
                          (bulkReplicateToParquet(newInsertsDF, "insert", currentTile),
                           bulkReplicateToParquet(newUpdatesDF, "update", currentTile),
                           bulkReplicateToParquet(newDeletesDF, "delete", currentTile))
                        }
                        logger.info(s"Delta completed for tile $currentTile: inserts=$inserted, updates=$updated, deletes=$deleted")
                        val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, LocalDateTime.now().toString)
                        putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
                      }
                    }

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
                    val inserted = if (jsonMapping4s.replication.useCustomSerializer) {
                      jsonToParquet(sourceDf, "insert", currentTile)
                    } else {
                      bulkReplicateToParquet(sourceDf, "insert", currentTile)
                    }
                    logger.info(s"Historical load completed for tile $currentTile: $inserted rows inserted")

                    val content = ReplicationStats(currentTile, inserted, 0, 0, 0, LocalDateTime.now().toString)
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

    def keysDiscoveryProcess(): Unit = {
      // 1. Infer PK columns for Iceberg table schema
      val pkColumnsForIceberg = inferKeys(internalConnectionToSource, "primaryKeys", srcKeyspaceName, srcTableName, columnTs)
        .map(m => { val (name, tpe) = m.head; Map("name" -> name, "type" -> tpe) })

      // 2. Backward compatibility: migrate legacy Parquet data to Iceberg if present
      if (hasLegacyParquetData(s3client, landingZone, srcKeyspaceName, srcTableName, totalTiles)) {
        logger.info(s"Legacy Parquet data detected for $srcKeyspaceName.$srcTableName, migrating to Iceberg")
        migrateParquetToIceberg(sparkSession, glueContext, s3client, landingZone, srcKeyspaceName, srcTableName, totalTiles, pkColumnsForIceberg)
      }

      // 3. Read primary keys from Cassandra via spark-cassandra-connector
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
        case ts if ts != "None" && replicationPointInTime > 0 => {
          val filterCon = jsonMapping4s.replication.pointInTimeReplicationConfig.predicateOp match {
            case "greaterThan" => col("ts") > replicationPointInTime && col("ts").isNotNull
            case "lessThan" => col("ts") < replicationPointInTime && col("ts").isNotNull
            case "equal" => col("ts") === replicationPointInTime && col("ts").isNotNull
            case _ => col("ts") > replicationPointInTime && col("ts").isNotNull
          }
          sparkSession.read.cassandraFormat(srcTableForDiscovery, srcKeyspaceName).option("inferSchema", "true").
            load().
            selectExpr(pkFinal.map(c => c): _*).
            filter(filterCon).
            persist(cachingMode)
        }
      }

      // 4. Group by xxhash64 % totalTiles and rename to 'tile'
      val groupedPkDF = primaryKeysDf
        .withColumn("tile", abs(xxhash64(pkFinalWithoutTs.map(c => col(c)): _*)) % totalTiles)
        .repartition(col("tile"))
        .persist(cachingMode)

      val ledgerConnection = getDBConnection("KeyspacesConnector.conf", bcktName, s3client)
      try {
        // 5. Create Iceberg database and tables sequentially to avoid Glue Catalog concurrency conflicts
        val dbName = s"$icebergCatalog.${srcKeyspaceName}_db"
        sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
        for (tile <- 0 until totalTiles) {
          ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
        }

        // 6. Write per-tile snapshots in parallel, record in ledger
        val tiles = (0 until totalTiles).toList.par
        tiles.foreach(tile => {
          logger.info(s"Processing tile $tile")
          val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, tile)

          // Check if replication has consumed the current snapshot for this tile
          val currEntry = Option(ledgerConnection.execute(
            s"SELECT load_status FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='curr'"
          ).one())
          val currLoadStatus = currEntry.map(r => Option(r.getString("load_status")).getOrElse("")).getOrElse("")

          if (currEntry.isDefined && currLoadStatus != "SUCCESS") {
            // Skip: replication hasn't consumed the current snapshot yet
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
            val content = DiscoveryStats(tile, tileCount, LocalDateTime.now().toString)
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
          case "discovery" => {
            keysDiscoveryProcess()
            if (workloadType.equals("batch")) {
              logger.info("The discovery job is completed")
              sys.exit()
            }
          }
          case "replication" => {
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