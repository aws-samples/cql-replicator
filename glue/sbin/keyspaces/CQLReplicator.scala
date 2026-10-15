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
import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, DriverOption}
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchStatementBuilder, DefaultBatchType, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.servererrors._
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.oss.driver.api.core.{AllNodesFailedException, CqlIdentifier, CqlSession, DriverException, NoNodeAvailableException}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.{Retry, RetryConfig}
import net.jpountz.lz4.{LZ4Compressor, LZ4CompressorWithLength, LZ4Factory}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.util.LongAccumulator
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
// Cross-account Keyspaces auth. Defined in a scoped `package` block further down
// this file, because the driver cannot reflectively load a default-package class.
import com.amazon.cqlreplicator.{CrossAccountSigV4AuthProvider => CrossAccountAuth}

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

/**
 * Identifier handling for case-sensitive (quoted) keyspace, table, and column
 * names.
 *
 * Four incompatible conventions are in play; mixing them up is the root cause
 * of the case-sensitivity defects, so every conversion goes through here:
 *
 *  - internal form -- exact case, no quotes ("MyCol" -> MyCol). What the driver
 *    metadata API returns from asInternal(), and what the
 *    spark-cassandra-connector expects in cassandraFormat/cassandraTable.
 *  - CQL form -- double-quoted when the name is not a bare lowercase
 *    identifier (MyCol -> "MyCol"). Required in CQL statement text AND used as
 *    the JSON key by both `SELECT JSON` and CustomResultSetSerializer.
 *  - Spark SQL form -- backtick-quoted. See toSparkName; do NOT feed CQL form
 *    to selectExpr/col.
 *  - Glue form -- lowercase-folded, since Glue Catalog / Iceberg names are not
 *    case-sensitive. See toGlueName.
 */
object CqlIdentifiers {

  /** True when the operator supplied an explicitly quoted CQL identifier. */
  def isQuoted(raw: String): Boolean = {
    val t = raw.trim
    t.length >= 2 && t.startsWith("\"") && t.endsWith("\"")
  }

  /** Converts operator input to the internal (exact-case, unquoted) form. */
  def toInternal(raw: String): String = {
    val t = raw.trim
    if (isQuoted(t)) t.substring(1, t.length - 1).replace("\"\"", "\"") else t
  }

  /**
   * Renders an internal name as CQL text, double-quoting only when required.
   *
   * Delegates to the driver rather than matching a regex: a hand-rolled
   * "is it all lowercase" test leaves CQL reserved words such as `order` and
   * `table` unquoted, which the server then refuses to parse.
   */
  def toCql(internal: String): String =
    CqlIdentifier.fromInternal(internal).asCql(true)

  /**
   * The JSON payload key for a column.
   *
   * Verified against Amazon Keyspaces: `SELECT JSON` keys a case-sensitive
   * column in quoted CQL form (`"MyCol"`) and a lowercase one bare
   * (`lowercol`), and `INSERT ... JSON` accepts only that same form.
   */
  def jsonKey(internal: String): String = toCql(internal)

  /**
   * The JSON payload key for the `ttl(...)` pseudo-column.
   *
   * Verified against Amazon Keyspaces: when the column is case-sensitive the
   * WHOLE pseudo-column is quoted while the inner argument stays bare --
   * `"ttl(MyTtl)"`, NOT `ttl("MyTtl")`. A column that needs no quoting yields
   * the fully bare `ttl(myttl)`, so the decision to quote is driven by the
   * column name alone, not by the parentheses.
   */
  def ttlJsonKey(internal: String): String = {
    val pseudoColumn = s"ttl($internal)"
    if (toCql(internal) == internal) pseudoColumn
    else "\"" + pseudoColumn.replace("\"", "\"\"") + "\""
  }

  /**
   * Renders an internal name for use in Spark SQL expressions, which quote
   * with backticks rather than double quotes.
   */
  def toSparkName(internal: String): String =
    "`" + internal.replace("`", "``") + "`"

  /**
   * Lowercased, identifier-safe name for Glue Catalog / Iceberg objects, which
   * are not case-sensitive.
   *
   * Folding is lossy, so a short hash of the internal name is appended
   * whenever folding actually changed something. Without it `MyTable` and
   * `mytable` -- or `my-table` and `my_table` -- would collide on a single
   * Iceberg table and silently interleave two tables' rows.
   *
   * Names that are already legal lowercase identifiers are returned unchanged,
   * so existing all-lowercase deployments keep their current Glue table names.
   * Names that needed folding do change, but replication of a case-sensitive
   * table could not previously complete at all, so there is no working
   * deployment to migrate.
   */
  def toGlueName(internal: String): String = {
    val folded = internal.toLowerCase.replaceAll("[^a-z0-9_]", "_")
    if (folded == internal) folded
    else s"${folded}_${Integer.toHexString(internal.hashCode)}"
  }
}


// =====================================================================
// Cross-account authentication for an Amazon Keyspaces SOURCE.
//
// `aws-sigv4-auth-cassandra-java-driver-plugin` 4.0.9 -- the version pinned by
// glue/bin/cqlreplicator -- reads exactly ONE driver option
// (`advanced.auth-provider.aws-region`) and SILENTLY IGNORES `aws-role-arn`.
// There is no newer release to upgrade to. So replicating from a Keyspaces
// table in another account requires a provider that reads the role ARN itself
// and hands assumed-role credentials to 4.0.9's
// (AwsCredentialsProvider, String) constructor.
//
// Referenced from the driver conf, NOT wired in code:
//
//   advanced.auth-provider {
//     class        = com.amazon.cqlreplicator.CrossAccountSigV4AuthProvider
//     aws-region   = us-east-1
//     aws-role-arn = "arn:aws:iam::<source-account>:role/<source-read-role>"
//   }
//
// Going through the conf is what makes this work on BOTH read paths: the
// direct CqlSession sessions and the spark-cassandra-connector, which builds
// its own sessions via CassandraConnectionFactory and would ignore any
// CqlSessionBuilder.withAuthProvider call.
//
// The class name MUST be fully qualified. The driver's Reflection.resolveClass
// resolves an unqualified name ONLY against its own package
// (com.datastax.oss.driver.internal.core.auth) and never tries the bare name,
// so a provider in the default package -- where every other top-level class in
// this file lives -- could never load. Hence the scoped `package` block, which
// leaves GlueApp (and `--class GlueApp`) exactly where it is.
//
// BACKWARD COMPATIBILITY: with no `aws-role-arn` this is behaviourally
// identical to SigV4AuthProvider and never touches STS at all, so it is safe
// to name in any conf, including a same-account KeyspacesConnector.conf.
// Migrations from self-managed Cassandra are unaffected: they keep using
// CassandraConnector.conf as-is and never load this class.
//
// Verified on the real Glue 5.0 runtime (see glue/sbin/keyspaces/smoke/):
// reflective load, the full STS closure, and a genuine cross-account
// AssumeRole whose credentials STS confirms belong to the target account.
// =====================================================================
package com.amazon.cqlreplicator {

  import com.datastax.oss.driver.api.core.CqlSession
  import com.datastax.oss.driver.api.core.auth.{AuthProvider, Authenticator}
  import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, DriverOption}
  import com.datastax.oss.driver.api.core.context.DriverContext
  import com.datastax.oss.driver.api.core.metadata.EndPoint
  import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf, DefaultConnectionFactory, MultiplexingSchemaListener, ProfileFileBasedContactInfo}
  import org.apache.spark.SparkEnv
  import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
  import software.aws.mcs.auth.SigV4AuthProvider

  import scala.util.Try

  /**
   * Raised when cross-account auth is configured but unusable.
   *
   * Deliberately declared here rather than reusing this file's
   * PreFlightCheckException: classes in the DEFAULT package are not accessible
   * from a named package, so the existing exception types cannot be referenced
   * from inside this block.
   */
  class CrossAccountAuthException(message: String) extends RuntimeException(message)

  /**
   * Delegating AuthProvider that adds cross-account role assumption to the
   * sigv4 plugin 4.0.9.
   *
   * Refresh needs no bespoke logic: SigV4Authenticator.initialResponse() calls
   * resolveCredentials() per authentication rather than once at construction,
   * so a background-refreshing STS provider covers multi-hour runs and
   * reconnects pick up rotated credentials.
   *
   * Serialization is a non-issue for the same reason the conf indirection is
   * used: neither StsClient nor the credentials provider is serializable, but
   * the driver constructs this class itself in whichever JVM loads the profile,
   * so the executor-side per-partition sessions get their own instance and no
   * Spark closure ever captures one.
   */
  class CrossAccountSigV4AuthProvider(context: DriverContext) extends AuthProvider {

    private val profile = context.getConfig.getDefaultProfile

    /**
     * DriverExecutionProfile is keyed by DriverOption, not by raw String, so
     * each path is wrapped in a tiny anonymous option -- the same technique the
     * 4.0.9 plugin uses for its own `aws-region`.
     */
    private def opt(path: String): DriverOption = new DriverOption {
      override def getPath: String = path
    }

    private val RegionOption = opt("advanced.auth-provider.aws-region")
    private val RoleArnOption = opt("advanced.auth-provider.aws-role-arn")

    private val region: String =
      if (profile.isDefined(RegionOption)) profile.getString(RegionOption) else ""

    private val roleArn: String =
      if (profile.isDefined(RoleArnOption)) profile.getString(RoleArnOption).trim else ""

    /**
     * GATE: with no role ARN, STS is never referenced and its classes are
     * never loaded. This is what keeps same-account and Cassandra-source
     * deployments working on any Glue image, whether or not it bundles the AWS
     * SDK v2 `sts` module.
     */
    private val delegate: SigV4AuthProvider =
      if (roleArn.isEmpty) {
        if (region.isEmpty) new SigV4AuthProvider() else new SigV4AuthProvider(region)
      } else {
        new SigV4AuthProvider(
          CrossAccountSigV4AuthProvider.assumeRoleCredentials(roleArn, region), region)
      }

    override def newAuthenticator(endPoint: EndPoint, serverAuthenticator: String): Authenticator =
      delegate.newAuthenticator(endPoint, serverAuthenticator)

    override def onMissingChallenge(endPoint: EndPoint): Unit =
      delegate.onMissingChallenge(endPoint)

    override def close(): Unit = delegate.close()
  }

  object CrossAccountSigV4AuthProvider {

    val ProviderFqn = "com.amazon.cqlreplicator.CrossAccountSigV4AuthProvider"
    val StockProviderFqn = "software.aws.mcs.auth.SigV4AuthProvider"

    val ConnectionFactoryParam = "spark.cassandra.connection.factory"
    val ConnectionFactoryFqn = "com.amazon.cqlreplicator.CrossAccountConnectionFactory"

    /**
     * The loader that holds this script's classes. Sessions must resolve
     * advanced.auth-provider.class through it: without
     * CqlSessionBuilder.withClassLoader the driver uses its own loader, which
     * on Glue executors cannot see the script jar (Spark adds that jar per
     * task, in a child loader). The provider then fails to load with
     * "Can't find class com.amazon.cqlreplicator.CrossAccountSigV4AuthProvider",
     * on executors only. This loader delegates to its parent, so the driver's
     * own classes resolve exactly as before.
     */
    def classLoader: ClassLoader = classOf[CrossAccountSigV4AuthProvider].getClassLoader

    private val StsClientClass = "software.amazon.awssdk.services.sts.StsClient"
    private val AssumeRoleRequestClass = "software.amazon.awssdk.services.sts.model.AssumeRoleRequest"
    private val StsProviderClass =
      "software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider"

    /** True when the AWS SDK v2 STS module is usable on this classpath. */
    def stsAvailable: Boolean = Try(Class.forName(StsClientClass)).isSuccess

    /**
     * Background-refreshing assumed-role credentials.
     *
     * ---------------------------------------------------------------------
     * WHY REFLECTION.
     *
     * A STATIC reference to StsClient would make this whole file
     * uncompilable on any Glue image that does not bundle the AWS SDK v2
     * `sts` module -- breaking every user of this script, including
     * Cassandra-to-Keyspaces migrations that never enable cross-account auth.
     * Glue 5.0 happens to ship a fat aws-sdk-java-bundle jar containing sts,
     * but that is an image detail, not a contract.
     *
     * Reflection keeps sts a RUNTIME dependency of one code path, reached
     * only when `aws-role-arn` is actually configured. The trade-off is that
     * these calls are not type-checked, so the reflective surface is kept
     * deliberately small and is exercised end-to-end by the probe under
     * glue/sbin/keyspaces/smoke/ against a real Glue runtime.
     * ---------------------------------------------------------------------
     */
    def assumeRoleCredentials(roleArn: String, region: String): AwsCredentialsProvider = {
      if (!stsAvailable) {
        throw new CrossAccountAuthException(
          s"advanced.auth-provider.aws-role-arn is set to '$roleArn', which requires the " +
            s"AWS SDK v2 STS module ($StsClientClass), but it is not on the classpath of this " +
            "Glue image. Either run on a Glue version that bundles the AWS SDK v2 " +
            "(Glue 5.0 does), or remove aws-role-arn to authenticate with the job role's " +
            "own credentials.")
      }

      val stsClient = newStsClient(region, credentials = None)

      val reqBuilder = staticCall(AssumeRoleRequestClass, "builder")
      invoke(reqBuilder, "roleArn", roleArn)
      invoke(reqBuilder, "roleSessionName", "cqlreplicator")
      invoke(reqBuilder, "durationSeconds", Integer.valueOf(3600))
      val request = invoke(reqBuilder, "build")

      val provBuilder = staticCall(StsProviderClass, "builder")
      invoke(provBuilder, "stsClient", stsClient)
      invoke(provBuilder, "refreshRequest", request)
      // Refresh ahead of expiry, so no bespoke refresh loop is needed.
      invoke(provBuilder, "asyncCredentialUpdateEnabled", java.lang.Boolean.TRUE)
      invoke(provBuilder, "build").asInstanceOf[AwsCredentialsProvider]
    }

    /**
     * An StsClient, optionally signing with the supplied credentials. Returned
     * as AnyRef because StsClient cannot be named statically.
     */
    def newStsClient(region: String, credentials: Option[AwsCredentialsProvider]): AnyRef = {
      val builder = staticCall(StsClientClass, "builder")
      if (region.nonEmpty) {
        val regionObj = Class.forName("software.amazon.awssdk.regions.Region")
          .getMethod("of", classOf[String]).invoke(null, region)
        invoke(builder, "region", regionObj)
      }
      credentials.foreach(c => invoke(builder, "credentialsProvider", c))
      invoke(builder, "build")
    }

    /** Invoke a no-arg static factory (`builder()`) on a class named at runtime. */
    private def staticCall(className: String, method: String): AnyRef =
      Class.forName(className).getMethod(method).invoke(null)

    /**
     * Invoke `name` on `target`, resolving overloads by arity then by whether
     * the parameter accepts the supplied argument. AWS SDK builders overload
     * most setters with a Consumer<Builder> variant, so arity alone is
     * ambiguous; primitives are matched via their wrapper because
     * Class.isInstance is always false for a primitive class.
     */
    private def invoke(target: AnyRef, name: String, args: AnyRef*): AnyRef = {
      val candidates = target.getClass.getMethods
        .filter(m => m.getName == name && m.getParameterCount == args.size)
      val chosen = candidates
        .find(m => m.getParameterTypes.zip(args).forall { case (p, a) => accepts(p, a) })
        .orElse(candidates.headOption)
        .getOrElse(throw new NoSuchMethodException(
          s"${target.getClass.getName}.$name/${args.size}"))
      chosen.setAccessible(true)
      chosen.invoke(target, args: _*)
    }

    private def accepts(param: Class[_], arg: AnyRef): Boolean =
      if (arg == null) !param.isPrimitive
      else if (!param.isPrimitive) param.isInstance(arg)
      else param.getName match {
        case "boolean" => arg.isInstanceOf[java.lang.Boolean]
        case "int" => arg.isInstanceOf[java.lang.Integer]
        case "long" => arg.isInstanceOf[java.lang.Long]
        case _ => false
      }
  }

  /**
   * spark-cassandra-connector session factory for a cross-account source.
   *
   * Identical to DefaultConnectionFactory for a profile-file conf, except that
   * the session resolves configured class names through
   * CrossAccountSigV4AuthProvider.classLoader (see there for why executors
   * need it). Any other contact-info kind is delegated unchanged.
   *
   * Selected through spark.cassandra.connection.factory. The connector resolves
   * the name on the Spark driver, where the script jar is visible, and ships
   * this object to executors inside CassandraConnectorConf; Spark deserializes
   * it with the task loader, which can see the script jar. getScanner is
   * inherited: DefaultConnectionFactory overrides it only for DSE continuous
   * paging, which Amazon Keyspaces does not support.
   */
  object CrossAccountConnectionFactory extends CassandraConnectionFactory {
    override def createSession(conf: CassandraConnectorConf): CqlSession = conf.contactInfo match {
      case ProfileFileBasedContactInfo(path) =>
        val appName = Option(SparkEnv.get).map(_.conf.getAppId).getOrElse("NoAppID")
        CqlSession.builder()
          .withClassLoader(CrossAccountSigV4AuthProvider.classLoader)
          .withConfigLoader(DriverConfigLoader.fromUrl(DefaultConnectionFactory.maybeGetLocalFile(path)))
          .withApplicationName(s"Spark-Cassandra-Connector-$appName")
          .withSchemaChangeListener(new MultiplexingSchemaListener())
          .build()
      case _ =>
        DefaultConnectionFactory.createSession(conf)
    }
  }
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

    // The callers pass internal (unquoted) column names, but the payload keys
    // these helpers match against are in CQL form, so a case-sensitive column
    // would never match and its blob/UDT repair would be skipped silently.
    val binKeys = binColumns.map(CqlIdentifiers.jsonKey)
    val udtKeys = utdColumns.map(CqlIdentifiers.jsonKey)

    val transformedResult = if (binColumns.isEmpty && utdColumns.isEmpty) {
      parse(input)
    } else {
      val json = parse(input)
      val correctedBins = if (binKeys.nonEmpty) correctEmptyBin(json, binKeys) else json
      val convertedUdt = if (udtKeys.nonEmpty) convertUDTtoText(correctedBins, udtKeys) else correctedBins
      convertedUdt
    }
    transformedResult
  }
}

case class FlushingSet(flushingClient: CqlSession, internalConfig: WriteConfiguration, dlqConfig: DlqConfig, logger: GlueLogger, persistedCounter: LongAccumulator) {

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
      case Success(_) => persistedCounter.add(1L)
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
    val stmtCount = batchStatement.size()
    val resTry = Try(Retry.decorateSupplier(retry, () => flushingClient.execute(batchStatement)).get())
    resTry match {
      case Success(_) => persistedCounter.add(stmtCount.toLong)
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
    // The left side accepts either a bare identifier or a double-quoted
    // (case-sensitive) one, since WHERE clauses now quote such column names.
    // With a bare-only `\w+` the match failed outright for quoted identifiers,
    // silently changing the large-object S3 key layout.
    val patternWhereClauseToMap: Regex =
      """(?:"(?:[^"]|"")*"|\w+)=['"]?(.*?)['"]?(?: and |$)""".r

    def convertCassandraKeyToGenericKey(input: String): String = {
      patternWhereClauseToMap.findAllIn(input).matchData.map { m => s"${m.group(1)}" }.mkString(":")
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
      // Also opened per partition on executors, where the driver's own loader
      // cannot see a provider class defined in this script.
      val connection = CqlSession.builder
        .withClassLoader(CrossAccountAuth.classLoader)
        .withConfigLoader(DriverConfigLoader.fromString(connectorConf))
        .build()
      connection
    }

    /**
     * Warns about auth-provider settings the driver would accept but silently
     * not honour.
     *
     * The motivating case: `aws-role-arn` set while `class` is still the stock
     * software.aws.mcs.auth.SigV4AuthProvider. Plugin 4.0.9 reads only
     * `aws-region`, so the role ARN is ignored WITHOUT any error and the job
     * authenticates as the Glue job role -- succeeding against a same-account
     * table and failing confusingly against a cross-account one.
     *
     * Read through the driver's own config loader rather than by scanning the
     * text. HOCON retains settings the driver has no DriverOption for -- which
     * is how the provider itself reads `aws-role-arn` -- so a typed lookup sees
     * everything a regex would, and unlike a regex it is scoped to the
     * auth-provider section. That matters: the shipped CassandraConnector.conf
     * sets an unqualified `class = ExponentialReconnectionPolicy` under
     * advanced.reconnection-policy, which a whole-file scan for `class =` would
     * misread as an auth-provider misconfiguration.
     *
     * Warn-only -- a parse failure must never block a replication run.
     */
    /** Returns the configured advanced.auth-provider.class, or "" if unreadable. */
    def checkAuthProviderConfig(connectionConfName: String, bucketName: String,
                                s3client: AmazonS3, logger: GlueLogger): String = Try {
      val conf = s3client.getObjectAsString(bucketName, s"artifacts/$connectionConfName")
      val profile = DriverConfigLoader.fromString(conf).getInitialConfig.getDefaultProfile

      def opt(path: String): DriverOption = new DriverOption {
        override def getPath: String = path
      }
      def readString(path: String): String = {
        val o = opt(path)
        if (profile.isDefined(o)) profile.getString(o).trim else ""
      }

      val roleArn = readString("advanced.auth-provider.aws-role-arn")
      val hasRoleArn = roleArn.nonEmpty
      val providerClass = readString("advanced.auth-provider.class")

      if (hasRoleArn && providerClass == CrossAccountAuth.StockProviderFqn) {
        logger.warn(
          s"$connectionConfName sets advanced.auth-provider.aws-role-arn but class is " +
            s"${CrossAccountAuth.StockProviderFqn}, which IGNORES it (plugin 4.0.9 reads only " +
            s"aws-region). The connection will authenticate as this Glue job's own role, NOT the " +
            s"role in aws-role-arn. For cross-account access set class = ${CrossAccountAuth.ProviderFqn}.")
      }

      if (hasRoleArn && providerClass == CrossAccountAuth.ProviderFqn && !CrossAccountAuth.stsAvailable) {
        logger.warn(
          s"$connectionConfName requests cross-account auth but the AWS SDK v2 STS module is not " +
            s"on the classpath of this Glue image; opening the connection will fail. Use Glue 5.0 " +
            s"or remove aws-role-arn.")
      }

      // An unqualified name resolves ONLY against the driver's own auth package
      // (com.datastax.oss.driver.internal.core.auth); the bare name is never
      // tried. Short names of the driver's built-in providers are therefore
      // legitimate, so only flag one that package does not contain.
      val driverAuthPackage = "com.datastax.oss.driver.internal.core.auth"
      if (providerClass.nonEmpty && !providerClass.contains(".") &&
        Try(Class.forName(s"$driverAuthPackage.$providerClass")).isFailure) {
        logger.warn(
          s"$connectionConfName sets advanced.auth-provider.class = '$providerClass', which is not " +
            s"fully qualified and is not a built-in driver provider. Unqualified names are resolved " +
            s"only against $driverAuthPackage, so this will fail to load. Use a fully-qualified " +
            s"class name -- e.g. ${CrossAccountAuth.ProviderFqn} for cross-account access.")
      }
      providerClass
    } match {
      case Failure(t) =>
        logger.info(s"Could not inspect auth-provider settings in $connectionConfName: ${t.getMessage}")
        ""
      case Success(providerClass) => providerClass
    }

    // The column arguments are quoted where required: inside CQL statement text
    // a case-sensitive name must appear as writetime("MyCol"). Note this differs
    // from the JSON key the server returns, where the whole pseudo-column is
    // quoted instead -- see CqlIdentifiers.ttlJsonKey.
    def buildWritetimeExpression(columns: Seq[String]): String = columns match {
      case Seq(single) => s"writetime(${CqlIdentifiers.toCql(single)}) as ts"
      case multiple    => multiple.map(c => s"writetime(${CqlIdentifiers.toCql(c)})")
        .mkString("greatest(", ", ", ") as ts")
    }

    /**
     * Renders a `selectExpr` element. Most elements are plain column names and
     * must be backtick-quoted so a case-sensitive name is not folded, but
     * `inferKeys("primaryKeysWithTS", ...)` appends the writetime expression
     * built by [[buildWritetimeExpression]] as one more element. That one is a
     * SQL expression, not an identifier, so it must be passed through verbatim.
     */
    def toSelectExpr(writetimeCols: Seq[String])(element: String): String =
      if (writetimeCols.nonEmpty && element == buildWritetimeExpression(writetimeCols)) element
      else CqlIdentifiers.toSparkName(element)

    def tableMetadata(cc: CqlSession, ks: String, tbl: String): TableMetadata =
      cc.getMetadata.getKeyspace(CqlIdentifier.fromInternal(ks)).get
        .getTable(CqlIdentifier.fromInternal(tbl)).get

    def inferKeys(cc: CqlSession, keyType: String, ks: String, tbl: String, columnTs: String, writetimeCols: Seq[String] = Seq.empty): Seq[Map[String, String]] = {
      val meta = tableMetadata(cc, ks, tbl)
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
      tableMetadata(cc, ks, tbl).
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
          normalizeMappingIdentifiers(parse(s).extract[JsonMapping])
      }
    }

    /**
     * Strips the optional surrounding double quotes from every column name in
     * the mapping, so operators can write "MyCol" the same way they do for
     * --src-table. Downstream code then holds internal names uniformly and
     * converts per consumer via CqlIdentifiers.
     */
    def normalizeMappingIdentifiers(m: JsonMapping): JsonMapping = {
      def norm(c: String): String = CqlIdentifiers.toInternal(c)
      val ks = m.keyspaces
      m.copy(
        replication = m.replication.copy(columns = m.replication.columns.map(norm)),
        keyspaces = ks.copy(
          compressionConfig = ks.compressionConfig.copy(
            compressNonPrimaryColumns = ks.compressionConfig.compressNonPrimaryColumns.map(norm),
            targetNameColumn = norm(ks.compressionConfig.targetNameColumn)),
          largeObjectsConfig = ks.largeObjectsConfig.copy(
            column = norm(ks.largeObjectsConfig.column),
            xref = norm(ks.largeObjectsConfig.xref)),
          transformation = ks.transformation.copy(
            addNonPrimaryKeyColumns = ks.transformation.addNonPrimaryKeyColumns.map(norm),
            transformExpressions = ks.transformation.transformExpressions.map(t =>
              t.copy(columnName = norm(t.columnName), alias = norm(t.alias)))),
          udtConversion = ks.udtConversion.copy(columns = ks.udtConversion.columns.map(norm))))
    }

    /**
     * Validates that every operator-supplied column name exists on the source
     * table, with a case-sensitivity hint when only the letter case differs.
     *
     * Without this, a mis-cased column name surfaces much later as a confusing
     * CQL error, or -- for the JSON payload paths -- as silently skipped work.
     */
    def preFlightCheckColumns(connection: CqlSession,
                              keyspace: String,
                              table: String,
                              namedColumns: Seq[(String, String)],
                              logger: GlueLogger): Unit = {
      val available = connection.getMetadata
        .getKeyspace(CqlIdentifier.fromInternal(keyspace)).get
        .getTable(CqlIdentifier.fromInternal(table)).get
        .getColumns.asScala.keys.map(_.asInternal).toList

      namedColumns.filter { case (_, c) => c.nonEmpty && c != "None" }.foreach { case (origin, col) =>
        if (!available.contains(col)) {
          val caseHint = available.filter(_.equalsIgnoreCase(col)) match {
            case Nil => s". Available columns: ${available.map(CqlIdentifiers.toCql).mkString(", ")}"
            case similar =>
              s". A column with different letter case exists: ${similar.map(CqlIdentifiers.toCql).mkString(", ")}." +
                s" Case-sensitive names must be double-quoted, e.g. $origin '${CqlIdentifiers.toCql(similar.head)}'"
          }
          val message = s"ERROR: the column ${CqlIdentifiers.toCql(col)} from $origin does not exist in " +
            s"${CqlIdentifiers.toCql(keyspace)}.${CqlIdentifiers.toCql(table)}$caseHint"
          logger.error(message)
          throw new PreFlightCheckException(message, -5)
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
          case false => {
            val meta = c1.get.getMetadata
            val ksMeta = meta.getKeyspace(CqlIdentifier.fromInternal(keyspace))
            ksMeta.isPresent match {
              case true => {
                ksMeta.get.getTable(CqlIdentifier.fromInternal(table)).isPresent match {
                  case true => {
                    logger.info(s"the $dir table ${CqlIdentifiers.toCql(table)} exists")
                  }
                  case false => {
                    // Surface the case-sensitivity hint: a same-name-different-case table is the
                    // usual cause, and the fix is to quote the name on the command line.
                    val tables = ksMeta.get.getTables.asScala.keys.map(_.asInternal).toList
                    val caseHint = tables.filter(_.equalsIgnoreCase(table)) match {
                      case Nil => ""
                      case similar =>
                        s". A table with different letter case exists: ${similar.map(CqlIdentifiers.toCql).mkString(", ")}." +
                          s" Case-sensitive names must be double-quoted, e.g. --src-table '${CqlIdentifiers.toCql(similar.head)}'"
                    }
                    logErrorAndExit(s"ERROR: the $dir table ${CqlIdentifiers.toCql(table)} does not exist in keyspace ${CqlIdentifiers.toCql(keyspace)}$caseHint", -1)
                  }
                }
              }
              case false => {
                val keyspaces = meta.getKeyspaces.asScala.keys.map(_.asInternal).toList
                val caseHint = keyspaces.filter(_.equalsIgnoreCase(keyspace)) match {
                  case Nil => ""
                  case similar =>
                    s". A keyspace with different letter case exists: ${similar.map(CqlIdentifiers.toCql).mkString(", ")}." +
                      s" Case-sensitive names must be double-quoted, e.g. --src-keyspace '${CqlIdentifiers.toCql(similar.head)}'"
                }
                logErrorAndExit(s"ERROR: the $dir keyspace ${CqlIdentifiers.toCql(keyspace)} does not exist$caseHint", -2)
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

    val srcTableName = CqlIdentifiers.toInternal(args("SOURCE_TBL"))
    val srcKeyspaceName = CqlIdentifiers.toInternal(args("SOURCE_KS"))
    val trgTableName = CqlIdentifiers.toInternal(args("TARGET_TBL"))
    val trgKeyspaceName = CqlIdentifiers.toInternal(args("TARGET_KS"))

    // CQL-quoted forms for use inside statement text.
    val srcTableNameCql = CqlIdentifiers.toCql(srcTableName)
    val srcKeyspaceNameCql = CqlIdentifiers.toCql(srcKeyspaceName)
    val trgTableNameCql = CqlIdentifiers.toCql(trgTableName)
    val trgKeyspaceNameCql = CqlIdentifiers.toCql(trgKeyspaceName)
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

    // Inspected BEFORE the connections are opened: an ignored aws-role-arn is
    // what makes a cross-account misconfiguration fail, so the warning has to be
    // in the log ahead of the failure it explains.
    val sourceAuthProvider = checkAuthProviderConfig("CassandraConnector.conf", bcktName, s3client, logger)
    checkAuthProviderConfig("KeyspacesConnector.conf", bcktName, s3client, logger)

    // Only a cross-account source switches connector sessions to the factory
    // that can load the provider on executors; other sources keep the default.
    val crossAccountSource = sourceAuthProvider == CrossAccountAuth.ProviderFqn
    if (crossAccountSource) {
      sparkSession.conf.set(CrossAccountAuth.ConnectionFactoryParam, CrossAccountAuth.ConnectionFactoryFqn)
      logger.info(s"Cross-account source: connector sessions use ${CrossAccountAuth.ConnectionFactoryFqn}")
    }

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
    // The alias prefix stays bare so col() reads it as a qualifier, while the
    // column part is backticked so a case-sensitive name is not folded and a
    // name containing a dot is not misread as a nested field.
    val cond = pks.map { x =>
      val c = CqlIdentifiers.toSparkName(x)
      col(s"head.$c") === col(s"tail.$c")
    }.reduce(_ && _)
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

    // Validate the operator-supplied column names now that the mapping is
    // parsed, so a mis-cased name fails fast with an actionable message.
    {
      val ksCfg = jsonMapping4s.keyspaces
      val named =
        writetimeColumns.map("--writetime-column" -> _) ++
          Seq("--ttl-column" -> ttlColumn) ++
          (if (jsonMapping4s.replication.allColumns) Seq.empty
           else jsonMapping4s.replication.columns.map("JSON_MAPPING replication.columns" -> _)) ++
          (if (ksCfg.udtConversion.enabled)
             ksCfg.udtConversion.columns.map("JSON_MAPPING udtConversion.columns" -> _)
           else Seq.empty) ++
          (if (ksCfg.largeObjectsConfig.enabled)
             Seq("JSON_MAPPING largeObjectsConfig.column" -> ksCfg.largeObjectsConfig.column)
           else Seq.empty) ++
          (if (ksCfg.compressionConfig.enabled && !ksCfg.compressionConfig.compressAllNonPrimaryColumns)
             ksCfg.compressionConfig.compressNonPrimaryColumns
               .map("JSON_MAPPING compressionConfig.compressNonPrimaryColumns" -> _)
           else Seq.empty) ++
          (if (ksCfg.transformation.enabled)
             ksCfg.transformation.transformExpressions
               .map("JSON_MAPPING transformExpressions.columnName" -> _.columnName)
           else Seq.empty)
      preFlightCheckColumns(internalConnectionToSource, srcKeyspaceName, srcTableName, named, logger)
    }

    val replicatedColumns = jsonMapping4s match {
      case JsonMapping(Replication(true, _, _, _, _, _, _), _) => "*"
      // Operator-supplied names are already internal form; render as CQL so
      // case-sensitive columns are quoted in the SELECT list.
      case rep => rep.replication.columns.map(CqlIdentifiers.toCql).mkString(",")
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
        val lstColumns = allColumnsFromSource.flatMap(_.keys).toSeq
          .map(CqlIdentifiers.toCql) :+ s"ttl(${CqlIdentifiers.toCql(ttlColumn)})"
        lstColumns.mkString(",")
      }
      case s if !(s.equals("None") || replicatedColumns.equals("*")) => {
        val lstColumns = s"$replicatedColumns,ttl(${CqlIdentifiers.toCql(ttlColumn)})"
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
        allColumnsFromSource.flatMap(_.keys).map(CqlIdentifiers.toCql) :+ writetimeExpr mkString ", "
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
      // These index into the JSON payload, whose keys are in CQL form.
      val columnName = CqlIdentifiers.jsonKey(localConfig.column)
      val bucket = removeS3prefixIfPresent(localConfig.bucket)
      val prefix = removeLeadingEndingSlashes(localConfig.prefix)
      val xrefColumnName = CqlIdentifiers.jsonKey(localConfig.xref)
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
        // Both branches yield internal names; they are compared against payload
        // keys below, so convert the whole set to payload-key form.
        val compressColumns = (jsonMapping4s.keyspaces.compressionConfig.compressAllNonPrimaryColumns match {
          case true => {
            val acfs = allColumnsFromSource.flatMap(_.keys).toSet
            val excludePks = columns.map(x => x._1.toString).toSet
            acfs.filterNot(excludePks.contains(_))
          }
          case _ => jsonMapping4s.keyspaces.compressionConfig.compressNonPrimaryColumns.toSet
        }).map(CqlIdentifiers.jsonKey)
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
            filteredJson merge JObject(
              CqlIdentifiers.jsonKey(jsonMapping4s.keyspaces.compressionConfig.targetNameColumn) ->
                JString(binToHex(compressedPayload)))
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
      val jvalueByKey = jvalue \ CqlIdentifiers.ttlJsonKey(ttlColumn)
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
      val filteredJson = filterOutColumn(jvalue, CqlIdentifiers.ttlJsonKey(ttlColumn))
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
        s"SELECT json $cls FROM $srcKeyspaceNameCql.$srcTableNameCql WHERE $wc"
      } else {
        s"SELECT $cls FROM $srcKeyspaceNameCql.$srcTableNameCql WHERE $wc"
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

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String, persistedCounter: LongAccumulator): Unit = {
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
        lazy val fl = FlushingSet(keyspacesConnPerPar, jsonMapping4s.keyspaces.writeConfiguration, dlqConfig, logger, persistedCounter)
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
            (currentJson \ CqlIdentifiers.jsonKey(rule.columnName)) match {
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
                    currentJson merge JObject(CqlIdentifiers.jsonKey(rule.alias) -> newValue)
                  } else {
                    currentJson
                      .removeField { case (k, _) => k == CqlIdentifiers.jsonKey(rule.columnName) }
                      .merge(JObject(CqlIdentifiers.jsonKey(rule.alias) -> newValue))
                  }
                } else {
                  currentJson.transformField {
                    case (k, _) if k == CqlIdentifiers.jsonKey(rule.columnName) => (k, newValue)
                  }
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
                  val setStmt = counterValues.map { m =>
                    val col = CqlIdentifiers.toCql(m._1)
                    s"$col=$col+${m._2}"
                  }.mkString(",")
                  val counterCqlStmt = s"UPDATE $trgKeyspaceNameCql.$trgTableNameCql SET $setStmt WHERE $whereClause"
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
                      whereConditions += s"${CqlIdentifiers.toCql(originalColName)}=$formattedValue"
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
                      whereConditions += s"${CqlIdentifiers.toCql(aliasColName)}=$formattedValue"
                    }
                  }

                  whereConditions.mkString(" and ")
                } else {
                  whereClause
                }
                val cqlStatement = s"DELETE FROM $trgKeyspaceNameCql.$trgTableNameCql WHERE $deleteWhereClause"
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
              val cqlStatement = s"INSERT INTO $trgKeyspaceNameCql.$trgTableNameCql JSON '$backToJsonRow' $tsSuffix$cas"
              if (maxStatementsPerBatch > 1)
                fl.add(cqlStatement)
              else
                fl.executeCQLStatement(cqlStatement)
            case _ =>
              val updatedJsonRow = compact(render(offloadToS3(parse(backToJsonRow), s3ClientOnPartition, whereClause)))
              val cqlStatement = s"INSERT INTO $trgKeyspaceNameCql.$trgTableNameCql JSON '$updatedJsonRow' $tsSuffix$cas"
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
              val cqlStatement = s"INSERT INTO $trgKeyspaceNameCql.$trgTableNameCql JSON '$backToJsonRow'$cas USING TTL $ttlVal"
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
              val cqlStatement = s"INSERT INTO $trgKeyspaceNameCql.$trgTableNameCql JSON '$backToJsonRow'$cas USING TTL $ttlVal"
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
        // colName stays in internal form above (Spark row lookups need it); the
        // CQL form is only used here, in the emitted statement text.
        whereStmt.append(s"${CqlIdentifiers.toCql(colName)}=$v")
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

    def persist(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String, historicalLoad: Boolean = false): Long = {
      val shaped =
        if (jsonMapping4s.keyspaces.writeConfiguration.preShuffleBeforeWrite) shuffleDfV2(df)
        else df.coalesce(defaultPartitions)
      // `shaped` is always a freshly derived DataFrame (repartition or
      // coalesce), so its storage level is always NONE and it is never
      // caller-cached: persist unconditionally and unpersist in the finally.
      val cached = shaped.persist(cachingMode)
      val persistedCounter = sparkContext.longAccumulator(s"persisted_${op}_tile_$tile")
      // Tracks how many rows have already been flushed to the per-tile
      // replication stats object, so each onTaskEnd callback and the
      // authoritative final flush below write only the incremental delta
      // exactly once.
      val lastReported = new java.util.concurrent.atomic.AtomicLong(0L)
      // Per-invocation lock so concurrent onTaskEnd callbacks and the
      // post-action flush are mutually exclusive (a delta is never lost or
      // double-counted) without serialising flushes across other tiles.
      val statsFlushLock = new Object()
      def flushReplicationStats(): Unit = statsFlushLock.synchronized {
        val current = persistedCounter.value
        val prev = lastReported.get
        if (current > prev) {
          val delta = current - prev
          lastReported.set(current)
          logger.info(s"Tile $tile $op: $current rows persisted so far")
          val primaryKeysDelta = if (historicalLoad) delta else 0L
          val content = op match {
            case "insert" => ReplicationStats(tile, primaryKeysDelta, 0, delta, 0, org.joda.time.LocalDateTime.now().toString)
            case "update" => ReplicationStats(tile, 0, delta, 0, 0, org.joda.time.LocalDateTime.now().toString)
            case "delete" => ReplicationStats(tile, 0, 0, 0, delta, org.joda.time.LocalDateTime.now().toString)
          }
          putStats(bcktName, s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)
        }
      }
      val statsListener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = flushReplicationStats()
      }
      sparkContext.addSparkListener(statsListener)
      try {
        persistToTarget(cached, columns, columnsPos, tile, op, persistedCounter)
        // Authoritative final flush. Once persistToTarget's action returns,
        // every successful task's accumulator update has been merged on the
        // driver, but the SparkListenerBus delivers onTaskEnd events
        // asynchronously -- the last events can still be in flight when the
        // listener is removed in `finally`, so their deltas would otherwise
        // never be written. Flushing here against the now-final accumulator
        // value guarantees the stats object reflects the full persisted
        // count (fixes the replicated-vs-discovered stats undercount).
        flushReplicationStats()
        val count = persistedCounter.value
        logger.info(s"Tile $tile $op: $count rows persisted to Amazon Keyspaces")
        count
      } finally {
        sparkContext.removeSparkListener(statsListener)
        cached.unpersist(blocking = false)
      }
    }

    // ======================== Iceberg Table Management Functions ========================

    /** Fully qualified Iceberg table name in Glue Catalog, per tile. */
    def icebergTableName(ksName: String, tblName: String, tile: Int): String = {
      // Glue Catalog databases/tables are lowercase-only, so mixed-case CQL names are folded
      // to a safe form. Must stay in sync with the cleanup path below.
      s"$icebergCatalog.${CqlIdentifiers.toGlueName(ksName)}_db.${CqlIdentifiers.toGlueName(tblName)}_tile_${tile}_pk_snapshots"
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
     * Location: s3://{landingZone}/{ks}/{tbl}/iceberg/tile_{tile}/ where {ks}
     * and {tbl} are Glue-folded, matching the catalog name.
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
        // Spark SQL DDL quotes with backticks, not double quotes.
        val name = CqlIdentifiers.toSparkName(c("name"))
        val sparkType = cassandraTypeToSparkSql(c("type"))
        s"$name $sparkType"
      }.mkString(", ")
      val lzPath = lz.stripSuffix("/")
      // Folded like the catalog name: using the raw names here would put two
      // case-differing tables that share one Glue table at different prefixes.
      val location = s"$lzPath/${CqlIdentifiers.toGlueName(ksName)}/${CqlIdentifiers.toGlueName(tblName)}/iceberg/tile_$tile/"
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
     *
     * Deliberately a full overwrite rather than `overwritePartitions()`. The tile table always
     * holds the complete current primary-key set for its tile, so replacing everything is the
     * intended semantics — and Iceberg skips a dynamic-overwrite commit outright when no data
     * files are produced (`SparkWrite.DynamicOverwrite.commit`: "Dynamic overwrite is empty,
     * skipping commit"). That left the snapshot frozen once a tile's key set went empty, so
     * `prev` and `curr` converged on the same snapshot id and the deletes that emptied the tile
     * could never be computed. `OverwriteByFilter` has no such guard and commits a correct
     * "all rows removed" snapshot.
     */
    def writeIcebergTileSnapshot(
      spark: SparkSession,
      tileDf: DataFrame,
      tableName: String
    ): Unit = {
      tileDf.writeTo(tableName).overwrite(lit(true))
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
    // Ledger statements run against Amazon Keyspaces, which can return a
    // transient ServerError that the driver's default retry policy does NOT
    // retry. One such error must not abort a discovery cycle, so every ledger
    // statement goes through this bounded exponential-backoff retry (same
    // exception set as FlushingSet uses for data writes).
    val ledgerRetry: Retry = {
      val r = Retry.of("ledger", RetryConfig.custom
        .maxAttempts(8)
        .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(100), 2.0))
        .retryExceptions(
          classOf[WriteFailureException],
          classOf[WriteTimeoutException],
          classOf[ServerError],
          classOf[UnavailableException],
          classOf[NoNodeAvailableException],
          classOf[AllNodesFailedException],
          classOf[DriverException]
        )
        .build())
      r.getEventPublisher.onRetry(event =>
        logger.warn(s"Retrying a ledger statement (attempt ${event.getNumberOfRetryAttempts}): " +
          s"${event.getLastThrowable.getMessage}"))
      r
    }

    def ledgerExecute(session: CqlSession, statement: String): com.datastax.oss.driver.api.core.cql.ResultSet =
      Retry.decorateSupplier(ledgerRetry, () => session.execute(statement)).get()

    def recordDiscoverySnapshot(
      session: CqlSession,
      ksName: String,
      tblName: String,
      tile: Int,
      snapshotId: Long
    ): Unit = {
      val existingCurr = Option(ledgerExecute(session,
        s"SELECT location, load_status FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$ksName' and tbl='$tblName' and tile=$tile and ver='curr'"
      ).one())

      existingCurr match {
        case Some(row) =>
          val oldSnapshotId = row.getString("location")
          val oldLoadStatus = Option(row.getString("load_status")).getOrElse("")
          ledgerExecute(session,
            s"BEGIN UNLOGGED BATCH " +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'$oldSnapshotId','prev','$oldLoadStatus',toTimestamp(now()));" +
              s"INSERT INTO $internalMigrationKeyspace.$internalMigrationTable(ks,tbl,tile,offload_status,dt_offload,location,ver,load_status,dt_load) VALUES('$ksName','$tblName',$tile,'SUCCESS',toTimestamp(now()),'${snapshotId.toString}','curr','','');" +
              s"APPLY BATCH;"
          )
        case None =>
          ledgerExecute(session,
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

                  val cnt = persist(sourceDfV2, columns, columnsPos, currentTile, "insert", historicalLoad = true)
                  logger.info(s"Historical load completed for tile $currentTile: $cnt rows inserted")

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
        case mv if mv.enabled => CqlIdentifiers.toInternal(mv.mvName)
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
            .selectExpr(filterColumns.map(toSelectExpr(writetimeColumns)): _*)
            .withColumn("ts", lit(0))

        case ts if ts != "None" && replicationPointInTime == 0 =>
          baseDF
            .selectExpr(filterColumns.map(toSelectExpr(writetimeColumns)): _*)

        case ts if ts != "None" && replicationPointInTime > 0 =>
          baseDF
            .selectExpr(filterColumns.map(toSelectExpr(writetimeColumns)): _*)
            .filter($"ts".isNotNull && pointInTimePredicate($"ts"))

        case ts if ts == "None" && counterColumns.nonEmpty =>
          val selectedColumns = pkFinal ++ counterColumns
          baseDF
            .selectExpr(selectedColumns.map(toSelectExpr(writetimeColumns)): _*)
            .withColumn("ts", lit(0))
            .withColumn("counter_hash",
              xxhash64(counterColumns.map(c => col(CqlIdentifiers.toSparkName(c))): _*))
      }

      val groupingExpr = abs(xxhash64(concat(
        pkFinalWithoutTs.map { colName =>
          // schema(...) is a literal field lookup and takes the internal name,
          // while col(...) parses its argument and needs the backticked form.
          val sparkCol = col(CqlIdentifiers.toSparkName(colName))
          baseDF.schema(colName).dataType match {
            case struct: StructType =>
              // For tuple types, extract fields and concatenate
              val fields = struct.fieldNames.map(field => sparkCol.getField(field).cast("string"))
              concat(lit("("), concat_ws(",", fields: _*), lit(")"))
            case _ =>
              sparkCol.cast("string")
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
        case mv if mv.enabled => CqlIdentifiers.toInternal(mv.mvName)
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
      val rddConf = sparkContext.getConf
        .set("spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")
        .set("spark.cassandra.input.consistency.level", jsonMapping4s.replication.readConfiguration.consistencyLevel)
      // getConf is a copy of the SparkConf and does not see session-level settings.
      if (crossAccountSource) rddConf.set(CrossAccountAuth.ConnectionFactoryParam, CrossAccountAuth.ConnectionFactoryFqn)
      val cassandraConnector = CassandraConnector(rddConf)
      
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
      val rs = ledgerExecute(ledgerSession,
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
          ledgerExecute(ledgerConnection, s"DELETE FROM $internalMigrationKeyspace.$internalMigrationTable WHERE ks='$srcKeyspaceName' AND tbl='$srcTableName'")

          // Task 3.2: Create Iceberg database and ensure all tile tables exist sequentially
          // (same pattern as keysDiscoveryProcess to avoid Glue Catalog concurrency conflicts)
          val dbName = s"$icebergCatalog.${CqlIdentifiers.toGlueName(srcKeyspaceName)}_db"
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
                  ledgerExecute(ledgerConnection,
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
                ledgerExecute(ledgerConnection,
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
        val dbName = s"$icebergCatalog.${CqlIdentifiers.toGlueName(srcKeyspaceName)}_db"
        sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
        for (tile <- 0 until totalTiles) {
          ensureIcebergTableExists(sparkSession, srcKeyspaceName, srcTableName, tile, pkColumnsForIceberg, landingZone)
        }

        // Data writes can run in parallel — each tile writes to its own independent table
        val tiles = (0 until totalTiles).toList.par
        val failedTiles = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
        tiles.foreach(tile => {
          try {
          logger.info(s"Processing tile $tile")
          val icebergTblName = icebergTableName(srcKeyspaceName, srcTableName, tile)

          // Check if replication has consumed the current snapshot for this tile
          val currEntry = Option(ledgerExecute(ledgerConnection,
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
          } catch {
            // One tile's failure must not abort the other tiles or kill the
            // job: the ledger was not advanced for this tile, so the next
            // discovery cycle picks it up again.
            case e: Throwable =>
              val sw = new java.io.StringWriter()
              e.printStackTrace(new java.io.PrintWriter(sw))
              logger.error(s"Tile $tile failed during discovery: ${e.getClass.getName}: ${e.getMessage}\n$sw")
              failedTiles.add(tile)
          }
        })
        if (!failedTiles.isEmpty) {
          val failed = failedTiles.asScala.toList.sorted.mkString(", ")
          if (workloadType.equals("batch")) {
            // A batch run ends after this pass, so a skipped tile would be
            // silently incomplete: fail the run.
            throw new RuntimeException(s"Discovery failed for tile(s) $failed")
          } else {
            logger.warn(s"Discovery failed for tile(s) $failed in this cycle; " +
              s"the ledger was not advanced for them, so the next cycle retries them")
          }
        }
      } finally {
        ledgerConnection.close()
      }
      groupedPkDF.unpersist()
      primaryKeysDf.unpersist()
    }

    cleanupLedger(internalConnectionToTarget, logger, srcKeyspaceName, srcTableName, cleanUpRequested, processType)

    // Clean up Iceberg tables and S3 data when --cr flag is used
    if (processType.equals("discovery") && cleanUpRequested) {
      val dbName = s"${CqlIdentifiers.toGlueName(srcKeyspaceName)}_db"

      // Use fully qualified SDK v2 classes to avoid conflicts with Glue ETL library
      val glueClient = software.amazon.awssdk.services.glue.GlueClient.create()

      try {
        // Delete per-tile Iceberg tables from Glue Catalog
        for (tile <- 0 until totalTiles) {
          val catalogTblName = s"${CqlIdentifiers.toGlueName(srcTableName)}_tile_${tile}_pk_snapshots"
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
