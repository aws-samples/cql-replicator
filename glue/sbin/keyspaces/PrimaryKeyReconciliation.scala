/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon Keyspaces — Primary-Key Reconciliation

import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{abs, asc_nulls_last, col, concat, hash, lit, md5, sha1, sha2, xxhash64}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Base64

class PreFlightCheckException(val message: String, val errorCode: Int, val cause: Throwable = null)
  extends Exception(s"[$errorCode] $message", cause) {
  def this(message: String) = this(message, 0)

  def this(message: String, cause: Throwable) = this(message, 0, cause)

  override def toString: String = s"PreFlightCheckException($errorCode, $message)"
}

class UnknownPKTransformRuleException(val rule: String)
  extends RuntimeException(
    s"Unknown PK_Transform rule '$rule'. Accepted rules (case-insensitive): " +
      "murmurhash3, md5, sha-1, sha-256, xxhash64."
  )


object PKTransformDispatcher {

  /**
   * Maps a `PK_Transform` rule to the Spark SQL function that produces the
   * post-transform value and to the resulting Spark `DataType`.
   *
   * @param rule case-insensitive rule name
   * @param col  source column to transform
   * @return     `(transformedColumn, postTransformDataType)` pair
   * @throws UnknownPKTransformRuleException when `rule` does not match any
   *         entry in the dispatch table
   */
  def apply(rule: String, col: Column): (Column, DataType) = {
    if (rule == null) throw new UnknownPKTransformRuleException("null")
    rule.toLowerCase match {
      case "murmurhash3" => (hash(col), IntegerType)
      case "md5"         => (md5(col), StringType)
      case "sha-1"       => (sha1(col), StringType)
      case "sha-256"     => (sha2(col, 256), StringType)
      case "xxhash64"    => (xxhash64(col), LongType)
      case _             => throw new UnknownPKTransformRuleException(rule)
    }
  }
}

class JsonMappingException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

final case class TransformExpression(columnName: String = "",
                                     rule: String = "",
                                     alias: String = "",
                                     keepSource: Boolean = false)

final case class Transformation(enabled: Boolean = false,
                                addNonPrimaryKeyColumns: List[String] = List(),
                                filterExpression: String = "",
                                transformExpressions: List[TransformExpression] = List())

final case class Keyspaces(transformation: Transformation = Transformation())

final case class JsonMapping(keyspaces: Keyspaces = Keyspaces())

final case class PKTransform(columnName: String,
                             rule: String,
                             alias: Option[String],
                             keepSource: Boolean)

/**
 * The resolved view of `JSON_MAPPING` consumed by the rest of the
 * Reconciliation_Job. `effectiveSourcePks` is the canonical schema
 * used by `Source_Scanner`, `Target_Scanner`, and `Diff_Engine` after
 * preflight; for the `"None"` / `enabled=false` short-circuits it
 * equals the source-side Primary_Key_Columns unchanged.
 *
 * @param filterExpr         the Spark SQL filter expression to apply to
 *                           the source DataFrame, or `None` when the
 *                           mapping omits or empties `filterExpression`.
 * @param filterColumns      source-side columns the source scan must
 *                           project alongside `effectiveSourcePks` to
 *                           evaluate `filterExpr`. Empty when no filter
 *                           is in effect or when no `addNonPrimaryKeyColumns`
 *                           are declared.
 * @param pkTransforms       the ordered list of PK_Transforms to apply
 *                           to the source DataFrame. Order matches the
 *                           operator-supplied `transformExpressions`
 *                           array (filtered to PK entries only).
 * @param effectiveSourcePks the post-transform PK column-name / type
 *                           pairs in declared order, with each
 *                           PK_Transform's column replaced in-place by
 *                           `(alias-or-columnName, post-transform-type)`.
 */
final case class EffectiveMapping(filterExpr: Option[String],
                                  filterColumns: Set[String],
                                  pkTransforms: Seq[PKTransform],
                                  effectiveSourcePks: Seq[(String, String)])

object JsonMappingResolver {

  val AcceptedRulesOrdered: Seq[String] =
    Seq("md5", "murmurhash3", "sha-1", "sha-256", "xxhash64")

  private val AcceptedRules: Set[String] = AcceptedRulesOrdered.toSet

  private def sparkTypeToCqlType(dt: DataType): String = dt match {
    case IntegerType => "int"
    case LongType    => "bigint"
    case StringType  => "text"
    case other       =>
      throw new JsonMappingException(
        s"Internal error: PKTransformDispatcher produced an unexpected DataType " +
          s"'${other.simpleString}' that does not map to a CQL type."
      )
  }

  /**
   * Resolves `rawJsonMapping` against `sourcePks` per the algorithm in
   * the object scaladoc. Throws [[JsonMappingException]] on any
   * structural-validation failure; the caller is responsible for
   * logging the exception class+message at `ERROR` and exiting with
   * the `JSON_MAPPING_FAIL` (E6) exit class.
   *
   * @param rawJsonMapping the raw `JSON_MAPPING` argument value. May be
   *                       `"None"` (the documented default), a base64
   *                       encoding of the JSON document (with arbitrary
   *                       inserted `\r`, `\n`, `\r\n` whitespace), or
   *                       `null` (treated identically to `"None"`).
   * @param sourcePks      the source-side `Primary_Key_Columns` as
   *                       `(name, type)` pairs in declared order, as
   *                       returned by `inferKeys(srcConn, "primaryKeys",
   *                       ks, tbl)`.
   * @param logger         used to emit the Req 19.5 INFO log line for
   *                       each non-PK `transformExpression` that is
   *                       dropped from `pkTransforms`.
   * @return the resolved [[EffectiveMapping]] consumed by
   *         `Source_Scanner`, `Target_Scanner`, and `Diff_Engine`.
   */
  def resolve(
      rawJsonMapping: String,
      sourcePks: Seq[(String, String)],
      logger: GlueLogger
  ): EffectiveMapping = {
    val baseMapping = EffectiveMapping(None, Set.empty, Seq.empty, sourcePks)

    if (rawJsonMapping == null || rawJsonMapping == "None") return baseMapping

    val decodedJson: String = Try {
      val stripped = rawJsonMapping.replaceAll("\\r\\n|\\r|\\n", "")
      new String(Base64.getDecoder.decode(stripped), StandardCharsets.UTF_8)
    } match {
      case Success(s) => s
      case Failure(t) =>
        throw new JsonMappingException(
          s"Failed to base64-decode JSON_MAPPING: ${t.getClass.getName}: ${t.getMessage}",
          t
        )
    }

    val decodedSentinel = decodedJson.replaceAll("\\r\\n|\\r|\\n", "").trim
    if (decodedSentinel.isEmpty || decodedSentinel == "None") return baseMapping

    val parsed: JsonMapping = Try {
      implicit val formats: DefaultFormats.type = DefaultFormats
      parse(decodedJson).extract[JsonMapping]
    } match {
      case Success(m) => m
      case Failure(t) =>
        throw new JsonMappingException(
          s"Failed to parse JSON_MAPPING as a JsonMapping document: " +
            s"${t.getClass.getName}: ${t.getMessage}",
          t
        )
    }

    val tx = parsed.keyspaces.transformation

    if (!tx.enabled) return baseMapping

    tx.transformExpressions
      .groupBy(_.columnName)
      .find { case (_, entries) => entries.size > 1 }
      .foreach { case (name, _) =>
        throw new JsonMappingException(
          s"Duplicate columnName '$name' in keyspaces.transformation.transformExpressions"
        )
      }

    tx.transformExpressions
      .find(t => t.keepSource && (t.alias == null || t.alias.isEmpty))
      .foreach { t =>
        throw new JsonMappingException(
          s"transformExpression for columnName='${t.columnName}' has " +
            s"keepSource=true but an empty alias; keepSource is meaningful " +
            s"only when alias is set."
        )
      }

    tx.transformExpressions.find { t =>
      val r = Option(t.rule).getOrElse("").toLowerCase
      !AcceptedRules.contains(r)
    }.foreach { t =>
      throw new JsonMappingException(
        s"transformExpression for columnName='${t.columnName}' uses unknown " +
          s"rule='${t.rule}'. Accepted rules (case-insensitive): " +
          AcceptedRulesOrdered.mkString("{", ",", "}")
      )
    }

    val filterExpr: Option[String] =
      Option(tx.filterExpression).map(_.trim).filter(_.nonEmpty)

    val sourcePkNames: Set[String] = sourcePks.map(_._1).toSet

    val filterColumns: Set[String] =
      tx.addNonPrimaryKeyColumns
        .filter(c => c != null && c.nonEmpty && !sourcePkNames.contains(c))
        .toSet

    val (pkExprs, nonPkExprs) = tx.transformExpressions
      .partition(t => sourcePkNames.contains(t.columnName))
    nonPkExprs.foreach { t =>
      logger.info(
        s"Ignoring transformExpression for columnName='${t.columnName}': " +
          s"non-primary-key column"
      )
    }

    val pkTransforms: Seq[PKTransform] = pkExprs.map { t =>
      val aliasOpt = Option(t.alias).filter(_.nonEmpty)
      PKTransform(t.columnName, t.rule, aliasOpt, t.keepSource)
    }

    val transformByCol: Map[String, PKTransform] =
      pkTransforms.map(t => t.columnName -> t).toMap

    val effectiveSourcePks: Seq[(String, String)] = sourcePks.map {
      case (name, origType) =>
        transformByCol.get(name) match {
          case Some(t) =>
            val (_, dt) = PKTransformDispatcher(t.rule, lit(""))
            val newName = t.alias.getOrElse(t.columnName)
            (newName, sparkTypeToCqlType(dt))
          case None =>
            (name, origType)
        }
    }

    EffectiveMapping(filterExpr, filterColumns, pkTransforms, effectiveSourcePks)
  }
}

object SchemaReconciliation {

  def flattenInferKeys(maps: Seq[Map[String, String]]): Seq[(String, String)] =
    maps.map(_.head)

  def renderKeySeq(seq: Seq[(String, String)]): String =
    seq.map { case (n, t) => s"($n,$t)" }.mkString("[", ", ", "]")

  def diffKeySequences(
      label: String,
      source: Seq[(String, String)],
      target: Seq[(String, String)]
  ): Seq[String] = {
    val errs = scala.collection.mutable.ArrayBuffer.empty[String]
    if (source.size != target.size) {
      errs += s"$label count mismatch: source has ${source.size} columns, target has ${target.size}"
    }
    val maxLen = math.max(source.size, target.size)
    var i = 0
    while (i < maxLen) {
      val sOpt = if (i < source.size) Some(source(i)) else None
      val tOpt = if (i < target.size) Some(target(i)) else None
      (sOpt, tOpt) match {
        case (Some(s), Some(t)) if s == t => ()
        case (Some((sN, sT)), Some((tN, tT))) =>
          errs += s"$label[$i]: source=($sN,$sT) target=($tN,$tT)"
        case (Some((sN, sT)), None) =>
          errs += s"$label[$i]: source=($sN,$sT) target=<absent>"
        case (None, Some((tN, tT))) =>
          errs += s"$label[$i]: source=<absent> target=($tN,$tT)"
        case (None, None) => ()
      }
      i += 1
    }
    errs.toSeq
  }

  sealed trait Outcome

  final case class Reconciled(
      sourcePartitionKeys: Seq[(String, String)],
      sourceClusteringKeys: Seq[(String, String)],
      effectivePartitionKeys: Seq[(String, String)],
      effectiveClusteringKeys: Seq[(String, String)],
      targetPartitionKeys: Seq[(String, String)],
      targetClusteringKeys: Seq[(String, String)]
  ) extends Outcome

  final case class EmptyPrimaryKeys(side: String) extends Outcome

  final case class PartitionKeyMismatch(differences: Seq[String]) extends Outcome

  final case class ClusteringKeyMismatch(differences: Seq[String]) extends Outcome

  def reconcile(
      srcPartitionKeys: Seq[(String, String)],
      srcPrimaryKeys: Seq[(String, String)],
      trgPartitionKeys: Seq[(String, String)],
      trgPrimaryKeys: Seq[(String, String)],
      mapping: EffectiveMapping
  ): Outcome = {

    if (srcPrimaryKeys.isEmpty) return EmptyPrimaryKeys("source")
    if (trgPrimaryKeys.isEmpty) return EmptyPrimaryKeys("target")

    val srcClusteringKeys: Seq[(String, String)] =
      srcPrimaryKeys.drop(srcPartitionKeys.size)
    val trgClusteringKeys: Seq[(String, String)] =
      trgPrimaryKeys.drop(trgPartitionKeys.size)

    val effectivePartitionKeys: Seq[(String, String)] =
      mapping.effectiveSourcePks.take(srcPartitionKeys.size)
    val effectiveClusteringKeys: Seq[(String, String)] =
      mapping.effectiveSourcePks.drop(srcPartitionKeys.size)

    val partitionDiffs =
      diffKeySequences("partitionKeyColumns", effectivePartitionKeys, trgPartitionKeys)
    if (partitionDiffs.nonEmpty) return PartitionKeyMismatch(partitionDiffs)

    val clusteringDiffs =
      diffKeySequences("clusteringKeyColumns", effectiveClusteringKeys, trgClusteringKeys)
    if (clusteringDiffs.nonEmpty) return ClusteringKeyMismatch(clusteringDiffs)

    Reconciled(
      srcPartitionKeys,
      srcClusteringKeys,
      effectivePartitionKeys,
      effectiveClusteringKeys,
      trgPartitionKeys,
      trgClusteringKeys
    )
  }
}

object SourceScanner {

  def readProjection(mapping: EffectiveMapping): Seq[String] = {
    val transformOutputNames: Set[String] =
      mapping.pkTransforms.map(t => t.alias.getOrElse(t.columnName)).toSet
    val passthrough: Seq[String] =
      mapping.effectiveSourcePks.map(_._1).filterNot(transformOutputNames.contains)
    val transformInputs: Seq[String] =
      mapping.pkTransforms.map(_.columnName)
    (passthrough ++ transformInputs ++ mapping.filterColumns.toSeq).distinct
  }

  def applyTransforms(df: DataFrame, mapping: EffectiveMapping): DataFrame =
    mapping.pkTransforms.foldLeft(df) { (acc, t) =>
      val (transformedCol, _) = PKTransformDispatcher(t.rule, col(t.columnName))
      t.alias match {
        case Some(aliasName) => acc.withColumn(aliasName, transformedCol)
        case None            => acc.withColumn(t.columnName, transformedCol)
      }
    }


  def projectEffective(df: DataFrame, mapping: EffectiveMapping): DataFrame =
    df.select(mapping.effectiveSourcePks.map { case (name, _) => col(name) }: _*)

  def groupColumn(effectivePkNames: Seq[String], totalTiles: Int): Column = {
    val parts: Seq[Column] = effectivePkNames.map(n => col(n).cast(StringType))
    (abs(xxhash64(concat(parts: _*))) % totalTiles).cast(IntegerType)
  }

  def applyPipeline(sourceDf: DataFrame, mapping: EffectiveMapping, totalTiles: Int): DataFrame = {
    val filtered = mapping.filterExpr.fold(sourceDf)(expr => sourceDf.filter(expr))
    val transformed = applyTransforms(filtered, mapping)
    val projected = projectEffective(transformed, mapping)
    projected
      .withColumn("group", groupColumn(mapping.effectiveSourcePks.map(_._1), totalTiles))
      .repartition(totalTiles, col("group"))
  }

  /**
   * @param spark          the active SparkSession.
   * @param srcConnConfName the Spark Cassandra Connector config profile file
   *                        name (`"CassandraConnector.conf"`) published under
   *                        `s3://<bucket>/artifacts/` and verified at bootstrap.
   * @param srcKs          source keyspace.
   * @param srcTbl         source table.
   * @param mapping        the resolved [[EffectiveMapping]] from
   *                       [[JsonMappingResolver.resolve]].
   * @param totalTiles     `TOTAL_TILES` in `[1, 1024]`.
   * @return a DataFrame whose schema is
   *         `Effective_Source_PK_Columns ++ ("group", IntegerType)`,
   *         repartitioned on `group` into `totalTiles` partitions.
   */
  def scan(
      spark: SparkSession,
      srcConnConfName: String,
      srcKs: String,
      srcTbl: String,
      mapping: EffectiveMapping,
      totalTiles: Int
  ): DataFrame = {
    val logger = new GlueLogger

    val readDf: DataFrame =
      try {
        spark.conf.set("spark.cassandra.connection.config.profile.path", srcConnConfName)
        val projection = readProjection(mapping)
        logger.info(
          s"SourceScanner: reading $srcKs.$srcTbl projecting " +
            s"${projection.size} column(s): ${projection.mkString("[", ", ", "]")}"
        )
        spark.read
          .cassandraFormat(srcTbl, srcKs)
          .load()
          .select(projection.map(col): _*)
      } catch {
        case NonFatal(t) =>
          logger.error(
            s"ERROR: SOURCE_SCAN_FAIL -- failed to establish the source session or " +
              s"project the primary-key columns for $srcKs.$srcTbl: " +
              s"${t.getClass.getName}: ${t.getMessage}"
          )
          sys.exit(PrimaryKeyReconciliation.SOURCE_SCAN_FAIL_EXIT_CODE)
      }

    val filteredDf: DataFrame =
      mapping.filterExpr match {
        case None => readDf
        case Some(expr) =>
          try {
            val f = readDf.filter(expr)
            f.queryExecution.analyzed
            f
          } catch {
            case NonFatal(t) =>
              logger.error(
                s"ERROR: FILTER_EXPR_FAIL -- Filter_Expression failed to parse or " +
                  s"references a column outside Primary_Key_Columns ∪ Filter_Columns. " +
                  s"expression='$expr': ${t.getClass.getName}: ${t.getMessage}"
              )
              sys.exit(PrimaryKeyReconciliation.FILTER_EXPR_FAIL_EXIT_CODE)
          }
      }

    try {
      val transformedDf = applyTransforms(filteredDf, mapping)
      val projectedDf = projectEffective(transformedDf, mapping)
      val groupedDf = projectedDf
        .withColumn("group", groupColumn(mapping.effectiveSourcePks.map(_._1), totalTiles))
      groupedDf.repartition(totalTiles, col("group"))
    } catch {
      case e: UnknownPKTransformRuleException =>
        logger.error(s"ERROR: JSON_MAPPING_FAIL -- ${e.getMessage}")
        sys.exit(PrimaryKeyReconciliation.JSON_MAPPING_FAIL_EXIT_CODE)
      case NonFatal(t) =>
        logger.error(
          s"ERROR: SOURCE_SCAN_FAIL -- failed to build the source transform pipeline " +
            s"for $srcKs.$srcTbl: ${t.getClass.getName}: ${t.getMessage}"
        )
        sys.exit(PrimaryKeyReconciliation.SOURCE_SCAN_FAIL_EXIT_CODE)
    }
  }
}

object TargetScanner {

  val ReadThrottleParam: String = "spark.cassandra.input.readsPerSec"

  def projectTarget(df: DataFrame, targetPks: Seq[(String, String)]): DataFrame =
    df.select(targetPks.map { case (name, _) => col(name) }: _*)

  def applyPipeline(targetDf: DataFrame, targetPks: Seq[(String, String)], totalTiles: Int): DataFrame = {
    val projected = projectTarget(targetDf, targetPks)
    projected
      .withColumn("group", SourceScanner.groupColumn(targetPks.map(_._1), totalTiles))
      .repartition(totalTiles, col("group"))
  }

  /**
   * Read the target primary-key columns, partition them on the same `group`
   * expression as `Source_Scanner`, and repartition on `group` into exactly
   * `totalTiles` partitions.
   *
   * @param spark          the active SparkSession.
   * @param trgConnConfName the Spark Cassandra Connector config profile file
   *                        name (`"KeyspacesConnector.conf"`) published under
   *                        `s3://<bucket>/artifacts/` and verified at bootstrap.
   * @param trgKs          target keyspace.
   * @param trgTbl         target table.
   * @param targetPks      the target-side primary-key `(name, type)` pairs in
   *                       declared order (after the Req 4.8 clustering-key
   *                       projection); names and types match
   *                       `Effective_Source_PK_Columns` per Req 4.5 / 4.6.
   * @param totalTiles     `TOTAL_TILES` in `[1, 1024]`.
   * @param maxTargetRcus  `MAX_TARGET_RCUS` in `[0, Int.MaxValue]`; `0`
   *                       disables throttling.
   * @return a DataFrame whose schema is `targetPks ++ ("group", IntegerType)`,
   *         repartitioned on `group` into `totalTiles` partitions.
   */
  def scan(
      spark: SparkSession,
      trgConnConfName: String,
      trgKs: String,
      trgTbl: String,
      targetPks: Seq[(String, String)],
      totalTiles: Int,
      maxTargetRcus: Int
  ): DataFrame = {
    val logger = new GlueLogger

    if (maxTargetRcus > 0) {
      try {
        spark.conf.set(ReadThrottleParam, maxTargetRcus.toString)
        logger.info(
          s"TargetScanner: configured read throttle $ReadThrottleParam=$maxTargetRcus " +
            s"(Keyspaces RCU units per second) before any target read"
        )
      } catch {
        case NonFatal(t) =>
          logger.error(
            s"ERROR: THROTTLE_CONFIG_FAIL -- could not apply the target read-throttle " +
              s"parameter '$ReadThrottleParam'=$maxTargetRcus to the SparkSession: " +
              s"${t.getClass.getName}: ${t.getMessage}"
          )
          sys.exit(PrimaryKeyReconciliation.THROTTLE_CONFIG_FAIL_EXIT_CODE)
      }
    } else {
      logger.info("TargetScanner: target throttling is disabled (MAX_TARGET_RCUS=0)")
    }

    logger.info(
      s"TargetScanner: starting target scan of $trgKs.$trgTbl with " +
        s"MAX_TARGET_RCUS=$maxTargetRcus, TOTAL_TILES=$totalTiles"
    )

    val readDf: DataFrame =
      try {
        spark.conf.set("spark.cassandra.connection.config.profile.path", trgConnConfName)
        val projection = targetPks.map(_._1)
        logger.info(
          s"TargetScanner: reading $trgKs.$trgTbl projecting " +
            s"${projection.size} primary-key column(s): ${projection.mkString("[", ", ", "]")}"
        )
        spark.read
          .cassandraFormat(trgTbl, trgKs)
          .load()
          .select(projection.map(col): _*)
      } catch {
        case NonFatal(t) =>
          logger.error(
            s"ERROR: TARGET_SCAN_FAIL -- failed to establish the target session or " +
              s"project the primary-key columns for $trgKs.$trgTbl: " +
              s"${t.getClass.getName}: ${t.getMessage}"
          )
          sys.exit(PrimaryKeyReconciliation.TARGET_SCAN_FAIL_EXIT_CODE)
      }

    try {
      applyPipeline(readDf, targetPks, totalTiles)
    } catch {
      case NonFatal(t) =>
        logger.error(
          s"ERROR: TARGET_SCAN_FAIL -- failed to build the target tile pipeline " +
            s"for $trgKs.$trgTbl: ${t.getClass.getName}: ${t.getMessage}"
        )
        sys.exit(PrimaryKeyReconciliation.TARGET_SCAN_FAIL_EXIT_CODE)
    }
  }
}

final case class DiffResult(
    sourceCount: Long,
    targetCount: Long,
    intersectionCount: Long,
    sourceMinusTargetCount: Long,
    targetMinusSourceCount: Option[Long],
    sourceMinusTargetSample: Seq[Row],
    targetMinusSourceSample: Option[Seq[Row]],
    sourceMinusTargetPath: String,
    targetMinusSourcePath: Option[String]
)

class DiffWriteVerificationException(val path: String, val expected: Long, val actual: Long)
  extends RuntimeException(
    s"Post-write verification mismatch at '$path': expected $expected row(s) " +
      s"but re-read $actual row(s) after the Parquet write reported success."
  )

object DiffEngine {

  val MaxDiffWriteAttempts: Int = 3

  def joinKeys(effectivePkCols: Seq[String]): Seq[String] = "group" +: effectivePkCols

  def antiJoinDistinct(
      leftDf: DataFrame,
      rightDf: DataFrame,
      effectivePkCols: Seq[String]
  ): DataFrame =
    leftDf
      .join(rightDf, joinKeys(effectivePkCols), "left_anti")
      .dropDuplicates(effectivePkCols)

  def intersectionDistinct(
      srcDf: DataFrame,
      trgDf: DataFrame,
      effectivePkCols: Seq[String]
  ): DataFrame =
    srcDf
      .join(trgDf, joinKeys(effectivePkCols), "inner")
      .dropDuplicates(effectivePkCols)

  def distinctCount(df: DataFrame): Long = df.distinct().count()

  /** `<prefix>diff/source_minus_target/` — the A\B diff sink (Req 7.1). */
  def sourceMinusTargetPath(prefix: String): String =
    s"${prefix}diff/source_minus_target/"

  /** `<prefix>diff/target_minus_source/` — the B\A diff sink, written only
    * when `BIDIRECTIONAL == true` (Req 7.2, 7.3). */
  def targetMinusSourcePath(prefix: String): String =
    s"${prefix}diff/target_minus_source/"

  def collectSample(
      diffDf: DataFrame,
      effectivePkCols: Seq[String],
      sampleSize: Int
  ): Seq[Row] = {
    if (sampleSize <= 0) Seq.empty
    else {
      val projected = diffDf.select(effectivePkCols.map(col): _*)
      val ordered = projected.orderBy(effectivePkCols.map(c => asc_nulls_last(c)): _*)
      ordered.limit(sampleSize).collect().toSeq
    }
  }

  private def writeDiffWithRetryAndVerify(
      spark: SparkSession,
      diffDf: DataFrame,
      path: String,
      expectedCount: Long,
      logger: GlueLogger
  ): Unit = {
    var attempt = 1
    var lastError: Throwable = null
    while (attempt <= MaxDiffWriteAttempts) {
      val outcome = Try {
        diffDf.write
          .mode("overwrite")
          .option("maxRecordsPerFile", PrimaryKeyReconciliation.KEYS_PER_PARQUET_FILE)
          .parquet(path)
        val writtenCount = spark.read.parquet(path).count()
        if (writtenCount != expectedCount) {
          throw new DiffWriteVerificationException(path, expectedCount, writtenCount)
        }
        writtenCount
      }
      outcome match {
        case Success(writtenCount) =>
          logger.info(
            s"DiffEngine: wrote and verified $writtenCount row(s) to $path " +
              s"(attempt $attempt/$MaxDiffWriteAttempts, mode=overwrite, " +
              s"maxRecordsPerFile=${PrimaryKeyReconciliation.KEYS_PER_PARQUET_FILE})"
          )
          return
        case Failure(t) =>
          lastError = t
          logger.warn(
            s"WARN: diff write to $path failed on attempt " +
              s"$attempt/$MaxDiffWriteAttempts: ${t.getClass.getName}: ${t.getMessage}"
          )
          attempt += 1
      }
    }

    logger.error(
      s"ERROR: DIFF_WRITE_FAIL -- the diff Parquet write to $path failed after " +
        s"$MaxDiffWriteAttempts attempt(s): ${lastError.getClass.getName}: " +
        s"${lastError.getMessage}. No report.json is written for this run."
    )
    cleanupDiffDir(path, logger)
    sys.exit(PrimaryKeyReconciliation.DIFF_WRITE_FAIL_EXIT_CODE)
  }

  private def cleanupDiffDir(path: String, logger: GlueLogger): Unit = {
    val (bucket, keyPrefix) = PrimaryKeyReconciliation.splitS3Location(path, "")
    Try {
      val s3ClientConf = new ClientConfiguration()
        .withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
      val s3client: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(s3ClientConf)
        .build()
      var listing = s3client.listObjectsV2(
        new ListObjectsV2Request().withBucketName(bucket).withPrefix(keyPrefix)
      )
      listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(bucket, obj.getKey))
      while (listing.isTruncated) {
        listing = s3client.listObjectsV2(
          new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(keyPrefix)
            .withContinuationToken(listing.getNextContinuationToken)
        )
        listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(bucket, obj.getKey))
      }
    } match {
      case Success(_) =>
        logger.info(s"Removed any partial diff directory under s3://$bucket/$keyPrefix")
      case Failure(t) =>
        logger.warn(
          s"WARN: best-effort cleanup of partial diff directory under " +
            s"s3://$bucket/$keyPrefix failed: ${t.getClass.getName}: ${t.getMessage}"
        )
    }
  }

  /**
   * Compute the source/target set arithmetic and write the diff Parquet
   * artifacts. See the object scaladoc and `design.md` -> "Components and
   * Interfaces" -> `Diff_Engine` for the full contract.
   * @param spark           the active SparkSession.
   * @param srcSnapshotDf   Source_Snapshot read back from `<prefix>source/`,
   *                        schema `effectivePkCols ++ ("group", IntegerType)`.
   * @param trgSnapshotDf   Target_Snapshot read back from `<prefix>target/`,
   *                        same schema.
   * @param effectivePkCols `Effective_Source_PK_Columns` names in declared
   *                        order (no `group`).
   * @param bidirectional   `BIDIRECTIONAL` — compute and write `B \ A` when
   *                        `true`.
   * @param sampleSize      `SAMPLE_SIZE` in `[0, 10000]`; `0` -> empty
   *                        samples.
   * @param prefix          the full `s3://<bucket>/<keyPrefix>/`
   *                        Reconciliation_Prefix URI (trailing `/`).
   * @return a fully-populated [[DiffResult]].
   */
  def compute(
      spark: SparkSession,
      srcSnapshotDf: DataFrame,
      trgSnapshotDf: DataFrame,
      effectivePkCols: Seq[String],
      bidirectional: Boolean,
      sampleSize: Int,
      prefix: String
  ): DiffResult = {
    val logger = new GlueLogger
    val sourceCount = distinctCount(srcSnapshotDf)
    val targetCount = distinctCount(trgSnapshotDf)
    logger.info(
      s"DiffEngine: sourceCount=$sourceCount, targetCount=$targetCount " +
        s"(set semantics over ${effectivePkCols.mkString("[", ", ", "]")})"
    )

    val smtPath = sourceMinusTargetPath(prefix)
    val srcMinusTargetDf = antiJoinDistinct(srcSnapshotDf, trgSnapshotDf, effectivePkCols).cache()
    val sourceMinusTargetCount = srcMinusTargetDf.count()
    writeDiffWithRetryAndVerify(spark, srcMinusTargetDf, smtPath, sourceMinusTargetCount, logger)
    val sourceMinusTargetSample = collectSample(srcMinusTargetDf, effectivePkCols, sampleSize)
    srcMinusTargetDf.unpersist()

    val (targetMinusSourceCount, targetMinusSourceSample, targetMinusSourcePathOpt) =
      if (bidirectional) {
        val tmsPath = targetMinusSourcePath(prefix)
        val trgMinusSrcDf = antiJoinDistinct(trgSnapshotDf, srcSnapshotDf, effectivePkCols).cache()
        val count = trgMinusSrcDf.count()
        writeDiffWithRetryAndVerify(spark, trgMinusSrcDf, tmsPath, count, logger)
        val sample = collectSample(trgMinusSrcDf, effectivePkCols, sampleSize)
        trgMinusSrcDf.unpersist()
        (Some(count), Some(sample), Some(tmsPath))
      } else {
        (None, None, None)
      }

    val intersectionCount =
      intersectionDistinct(srcSnapshotDf, trgSnapshotDf, effectivePkCols).count()

    logger.info(
      s"DiffEngine: sourceMinusTargetCount=$sourceMinusTargetCount, " +
        s"targetMinusSourceCount=${targetMinusSourceCount.map(_.toString).getOrElse("<omitted>")}, " +
        s"intersectionCount=$intersectionCount"
    )

    DiffResult(
      sourceCount = sourceCount,
      targetCount = targetCount,
      intersectionCount = intersectionCount,
      sourceMinusTargetCount = sourceMinusTargetCount,
      targetMinusSourceCount = targetMinusSourceCount,
      sourceMinusTargetSample = sourceMinusTargetSample,
      targetMinusSourceSample = targetMinusSourceSample,
      sourceMinusTargetPath = smtPath,
      targetMinusSourcePath = targetMinusSourcePathOpt
    )
  }
}

final case class FailureBlock(exceptionClass: String, message: String)

final case class ReportInputs(
    prefix: String,                                 // full s3://<bucket>/<keyPrefix> URI, trailing '/'
    runId: String,
    runStartedAt: String,                           // ISO-8601 UTC ms, "Z"
    runFinishedAt: String,
    srcKs: String, srcTbl: String,
    trgKs: String, trgTbl: String,
    partitionKeyColumns: Seq[(String, String)],     // (name, type) — see assumption (2)
    clusteringKeyColumns: Seq[(String, String)],
    schemaFingerprint: String,
    totalTiles: Int,
    diff: Option[DiffResult],                        // None before the diff phase — see assumption (1)
    status: String,                                 // "success" | "partial"
    phase: Option[String],                          // present when status == "partial"
    failure: Option[FailureBlock],
    jsonMapping: Option[JsonMapping],                // resolved object, not raw base64 (Req 19.9)
    sourceSnapshotPath: String,
    targetSnapshotPath: String,
    bidirectional: Boolean,
    loggingLevel: String = "INFO"
)


object ReportWriter {

  val MaxFailureMessageChars: Int = 4096


  private def rowToJson(row: Row, pkColNames: Seq[String]): JObject = {
    val names: Seq[String] =
      if (row.schema != null && row.schema.fieldNames.nonEmpty) row.schema.fieldNames.toSeq
      else pkColNames
    val fields: List[JField] = names.toList.zipWithIndex.map { case (name, i) =>
      val value: JValue =
        if (i < row.length && row.get(i) != null) JString(row.get(i).toString)
        else JNull
      JField(name, value)
    }
    JObject(fields)
  }

  def buildJson(inputs: ReportInputs): String = {
    // Build the JSON AST. Always-present metadata first; conditional fields
    // (diff-derived, bidirectional, partial `phase`/`failure`, resolved
    // `jsonMapping`) are appended only when applicable so the partial report
    // omits fields it has no data for.
    val keyTypeArr: Seq[(String, String)] => JArray = pairs =>
      JArray(pairs.toList.map { case (n, t) =>
        JObject(JField("name", JString(n)), JField("type", JString(t)))
      })

    val baseFields: List[JField] = List(
      JField("runId", JString(inputs.runId)),
      JField("runStartedAt", JString(inputs.runStartedAt)),
      JField("runFinishedAt", JString(inputs.runFinishedAt)),
      JField("sourceKeyspace", JString(inputs.srcKs)),
      JField("sourceTable", JString(inputs.srcTbl)),
      JField("targetKeyspace", JString(inputs.trgKs)),
      JField("targetTable", JString(inputs.trgTbl)),
      JField("partitionKeyColumns", keyTypeArr(inputs.partitionKeyColumns)),
      JField("clusteringKeyColumns", keyTypeArr(inputs.clusteringKeyColumns)),
      JField("schemaFingerprint", JString(inputs.schemaFingerprint)),
      JField("totalTiles", JInt(BigInt(inputs.totalTiles))),
      JField("sourceSnapshotPath", JString(inputs.sourceSnapshotPath)),
      JField("targetSnapshotPath", JString(inputs.targetSnapshotPath)),
      JField("status", JString(inputs.status))
    )

    val effectivePkColNames: Seq[String] =
      inputs.partitionKeyColumns.map(_._1) ++ inputs.clusteringKeyColumns.map(_._1)

    val diffFields: List[JField] = inputs.diff match {
      case None => Nil
      case Some(d) =>
        val core = List(
          JField("sourceCount", JInt(BigInt(d.sourceCount))),
          JField("targetCount", JInt(BigInt(d.targetCount))),
          JField("intersectionCount", JInt(BigInt(d.intersectionCount))),
          JField("sourceMinusTargetCount", JInt(BigInt(d.sourceMinusTargetCount))),
          JField("sourceMinusTargetSample",
            JArray(d.sourceMinusTargetSample.toList.map(r => rowToJson(r, effectivePkColNames)))),
          JField("sourceMinusTargetPath", JString(d.sourceMinusTargetPath))
        )
        val bidir: List[JField] =
          if (inputs.bidirectional) {
            List(
              JField("targetMinusSourceCount",
                d.targetMinusSourceCount.map(c => JInt(BigInt(c))).getOrElse(JNull)),
              JField("targetMinusSourceSample",
                JArray(d.targetMinusSourceSample.getOrElse(Seq.empty)
                  .toList.map(r => rowToJson(r, effectivePkColNames)))),
              JField("targetMinusSourcePath",
                d.targetMinusSourcePath.map(JString(_)).getOrElse(JNull))
            )
          } else Nil
        core ++ bidir
    }

    val partialFields: List[JField] =
      inputs.phase.map(p => JField("phase", JString(p))).toList ++
        inputs.failure.map { f =>
          val truncated =
            if (f.message != null && f.message.length > MaxFailureMessageChars)
              f.message.substring(0, MaxFailureMessageChars)
            else Option(f.message).getOrElse("")
          JField("failure", JObject(
            JField("exceptionClass", JString(f.exceptionClass)),
            JField("message", JString(truncated))
          ))
        }.toList

    val jsonMappingField: List[JField] = inputs.jsonMapping.map { jm =>
      implicit val formats: DefaultFormats.type = DefaultFormats
      JField("jsonMapping", Extraction.decompose(jm))
    }.toList

    val doc = JObject(baseFields ++ diffFields ++ partialFields ++ jsonMappingField)
    compact(render(doc))
  }

  def buildLogSummary(inputs: ReportInputs): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val full = parse(buildJson(inputs))
    val redacted = full.removeField {
      case JField("sourceMinusTargetSample", _) => true
      case JField("targetMinusSourceSample", _) => true
      case _                                    => false
    }
    compact(render(redacted))
  }

  private def defaultS3Client(): AmazonS3 = {
    val s3ClientConf = new ClientConfiguration()
      .withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(s3ClientConf)
      .build()
  }

  def write(inputs: ReportInputs): Unit =
    write(inputs, defaultS3Client())

  def write(inputs: ReportInputs, s3client: AmazonS3): Unit = {
    val logger = new GlueLogger
    val (bucket, keyPrefix) = PrimaryKeyReconciliation.splitS3Location(inputs.prefix, "")
    val reportKey = s"${keyPrefix}report.json"

    try {
      val json = buildJson(inputs)
      s3client.putObject(bucket, reportKey, json)
      logger.info(
        s"ReportWriter: wrote ${inputs.status} Report_Document to " +
          s"s3://$bucket/$reportKey"
      )
      PrimaryKeyReconciliation.logAtLevel(
        logger,
        inputs.loggingLevel,
        s"ReportWriter: Report_Document summary (primary-key samples redacted; " +
          s"full report at s3://$bucket/$reportKey): ${buildLogSummary(inputs)}"
      )
    } catch {
      case NonFatal(t) =>

        logger.error(
          s"ERROR: REPORT_WRITE_FAIL -- failed to serialize or PUT the " +
            s"Report_Document to s3://$bucket/$reportKey: " +
            s"${t.getClass.getName}: ${t.getMessage}"
        )
        sys.exit(PrimaryKeyReconciliation.REPORT_WRITE_FAIL_EXIT_CODE)
    }
  }
}

object PrimaryKeyReconciliation {

  val BAD_INPUT_EXIT_CODE: Int = 1
  val BOOTSTRAP_FAIL_EXIT_CODE: Int = 2
  val PREFLIGHT_FAIL_EXIT_CODE: Int = 3
  val PARTITION_KEY_MISMATCH_EXIT_CODE: Int = 4
  val CLUSTERING_KEY_MISMATCH_EXIT_CODE: Int = 5
  val JSON_MAPPING_FAIL_EXIT_CODE: Int = 6
  val FILTER_EXPR_FAIL_EXIT_CODE: Int = 7
  val THROTTLE_CONFIG_FAIL_EXIT_CODE: Int = 8
  val SOURCE_SCAN_FAIL_EXIT_CODE: Int = 9
  val TARGET_SCAN_FAIL_EXIT_CODE: Int = 10
  val DIFF_WRITE_FAIL_EXIT_CODE: Int = 11
  val REPORT_WRITE_FAIL_EXIT_CODE: Int = 12
  val READ_ONLY_VIOLATION_EXIT_CODE: Int = 13
  val RUN_ID_COLLISION_EXIT_CODE: Int = 14
  val KEYS_PER_PARQUET_FILE: Int = 10500000

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

  /** Parsed and validated argument bundle returned by [[parseArgs]]. */
  final case class ResolvedArgs(
      jobName: String,
      sourceKs: String,
      sourceTbl: String,
      targetKs: String,
      targetTbl: String,
      s3LandingZone: String,
      totalTiles: Int,
      logging: String,
      runId: String,
      keepSnapshots: Boolean,
      bidirectional: Boolean,
      sampleSize: Int,
      maxTargetRcus: Int,
      outputPrefixOverride: String,
      jsonMapping: String,
      jobRunId: String,
      raw: Map[String, String]
  )

  val RequiredArgNames: Seq[String] = Seq(
    "JOB_NAME",
    "SOURCE_KS",
    "SOURCE_TBL",
    "TARGET_KS",
    "TARGET_TBL",
    "S3_LANDING_ZONE",
    "TOTAL_TILES",
    "LOGGING"
  )

  val OptionalArgNames: Seq[String] = Seq(
    "RUN_ID",
    "KEEP_SNAPSHOTS",
    "BIDIRECTIONAL",
    "SAMPLE_SIZE",
    "MAX_TARGET_RCUS",
    "OUTPUT_PREFIX_OVERRIDE",
    "JSON_MAPPING"
  )

  val DefaultKeepSnapshots: Boolean = true
  val DefaultBidirectional: Boolean = true   // OQ7
  val DefaultSampleSize: Int = 100           // OQ6
  val DefaultMaxTargetRcus: Int = 0          // OQ3 (no throttle)
  val DefaultOutputPrefixOverride: String = ""
  val DefaultJsonMapping: String = "None"

  // TOTAL_TILES bounds
  val MinTotalTiles: Int = 1
  val MaxTotalTiles: Int = 1024

  // LOGGING enum
  val AcceptedLoggingLevels: Set[String] =
    Set("ERROR", "WARN", "INFO", "DEBUG")

  // RUN_ID rules: non-empty, <= 128 chars, [A-Za-z0-9_-] only.
  val MaxRunIdLength: Int = 128
  val RunIdPattern: Regex = "^[A-Za-z0-9_-]+$".r

  def lookupOptional(sysArgs: Array[String], argName: String): Option[String] = {
    val flag = s"--$argName"
    var i = 0
    while (i < sysArgs.length - 1) {
      if (sysArgs(i) == flag) return Option(sysArgs(i + 1)).map(_.trim)
      i += 1
    }
    None
  }

  def validateArgs(sysArgs: Array[String]): Either[Seq[String], ResolvedArgs] = {
    val resolved: Map[String, String] = Try {
      GlueArgParser
        .getResolvedOptions(sysArgs, RequiredArgNames.toArray)
        .toMap
    } match {
      case Success(m) => m
      case Failure(_) =>
        val manual = scala.collection.mutable.Map.empty[String, String]
        var i = 0
        while (i < sysArgs.length - 1) {
          if (sysArgs(i).startsWith("--")) {
            manual(sysArgs(i).drop(2)) = sysArgs(i + 1)
          }
          i += 1
        }
        manual.toMap
    }

    val errors = scala.collection.mutable.ArrayBuffer.empty[String]
    val resolvedRequired: Map[String, String] = RequiredArgNames.map { name =>
      val v = resolved.getOrElse(name, "")
      val trimmed = if (v == null) "" else v.trim
      if (trimmed.isEmpty) {
        errors += s"ERROR: required argument $name is missing or empty"
      }
      name -> trimmed
    }.toMap

    val rawRunId = lookupOptional(sysArgs, "RUN_ID").map(_.trim).filter(_.nonEmpty)
    val rawKeepSnapshots = lookupOptional(sysArgs, "KEEP_SNAPSHOTS").map(_.trim).filter(_.nonEmpty)
    val rawBidirectional = lookupOptional(sysArgs, "BIDIRECTIONAL").map(_.trim).filter(_.nonEmpty)
    val rawSampleSize = lookupOptional(sysArgs, "SAMPLE_SIZE").map(_.trim).filter(_.nonEmpty)
    val rawMaxRcus = lookupOptional(sysArgs, "MAX_TARGET_RCUS").map(_.trim).filter(_.nonEmpty)
    val rawOutputOverride = lookupOptional(sysArgs, "OUTPUT_PREFIX_OVERRIDE").getOrElse(DefaultOutputPrefixOverride)
    val rawJsonMapping = lookupOptional(sysArgs, "JSON_MAPPING").filter(_.nonEmpty).getOrElse(DefaultJsonMapping)

    val totalTiles: Int = {
      val raw = resolvedRequired.getOrElse("TOTAL_TILES", "")
      if (raw.isEmpty) -1
      else
        Try(raw.toInt) match {
          case Success(n) if n >= MinTotalTiles && n <= MaxTotalTiles => n
          case Success(n) =>
            errors += s"ERROR: TOTAL_TILES=$n is outside the accepted inclusive range [$MinTotalTiles, $MaxTotalTiles]"
            n
          case Failure(_) =>
            errors += s"ERROR: TOTAL_TILES='$raw' is not a parsable integer (accepted range: [$MinTotalTiles, $MaxTotalTiles])"
            -1
        }
    }

    val logging: String = {
      val raw = resolvedRequired.getOrElse("LOGGING", "")
      if (raw.isEmpty) ""
      else {
        val upper = raw.toUpperCase
        if (AcceptedLoggingLevels.contains(upper)) upper
        else {
          errors += s"ERROR: LOGGING='$raw' is not one of the accepted values ${AcceptedLoggingLevels.toSeq.sorted.mkString("{", ",", "}")} (case-insensitive)"
          upper
        }
      }
    }

    val jobRunId: String = lookupOptional(sysArgs, "JOB_RUN_ID")
      .orElse(resolved.get("JOB_RUN_ID").map(_.trim))
      .getOrElse("")
    val effectiveRunId: String = rawRunId.getOrElse(jobRunId)
    val runId: String =
      if (effectiveRunId.isEmpty) {
        errors += "ERROR: RUN_ID is empty and JOB_RUN_ID was not supplied by the Glue runtime"
        ""
      } else if (effectiveRunId.length > MaxRunIdLength) {
        errors += s"ERROR: RUN_ID has ${effectiveRunId.length} characters (maximum is $MaxRunIdLength)"
        effectiveRunId
      } else if (RunIdPattern.findFirstIn(effectiveRunId).isEmpty) {
        errors += s"ERROR: RUN_ID='$effectiveRunId' contains characters outside the accepted set [A-Za-z0-9_-]"
        effectiveRunId
      } else effectiveRunId

    val maxTargetRcus: Int = rawMaxRcus match {
      case None => DefaultMaxTargetRcus
      case Some(raw) =>
        Try(raw.toInt) match {
          case Success(n) if n >= 0 => n
          case Success(n) =>
            errors += s"ERROR: MAX_TARGET_RCUS=$n is not a non-negative integer (accepted range: [0, Int.MaxValue])"
            n
          case Failure(_) =>
            errors += s"ERROR: MAX_TARGET_RCUS='$raw' is not a parsable integer (accepted range: [0, Int.MaxValue])"
            DefaultMaxTargetRcus
        }
    }

    def parseBoolean(name: String, raw: Option[String], default: Boolean): Boolean = raw match {
      case None => default
      case Some(v) =>
        v.toLowerCase match {
          case "true"  => true
          case "false" => false
          case _ =>
            errors += s"ERROR: $name='$v' is not a parsable boolean (accepted values: {true,false}, case-insensitive)"
            default
        }
    }

    val keepSnapshots = parseBoolean("KEEP_SNAPSHOTS", rawKeepSnapshots, DefaultKeepSnapshots)
    val bidirectional = parseBoolean("BIDIRECTIONAL", rawBidirectional, DefaultBidirectional)

    val sampleSize: Int = rawSampleSize match {
      case None => DefaultSampleSize
      case Some(raw) =>
        Try(raw.toInt) match {
          case Success(n) if n >= 0 && n <= 10000 => n
          case Success(n) =>
            errors += s"ERROR: SAMPLE_SIZE=$n is outside the accepted inclusive range [0, 10000]"
            n
          case Failure(_) =>
            errors += s"ERROR: SAMPLE_SIZE='$raw' is not a parsable integer (accepted range: [0, 10000])"
            DefaultSampleSize
        }
    }

    if (errors.nonEmpty) {
      return Left(errors.toSeq)
    }

    Right(ResolvedArgs(
      jobName = resolvedRequired("JOB_NAME"),
      sourceKs = resolvedRequired("SOURCE_KS"),
      sourceTbl = resolvedRequired("SOURCE_TBL"),
      targetKs = resolvedRequired("TARGET_KS"),
      targetTbl = resolvedRequired("TARGET_TBL"),
      s3LandingZone = resolvedRequired("S3_LANDING_ZONE"),
      totalTiles = totalTiles,
      logging = logging,
      runId = runId,
      keepSnapshots = keepSnapshots,
      bidirectional = bidirectional,
      sampleSize = sampleSize,
      maxTargetRcus = maxTargetRcus,
      outputPrefixOverride = rawOutputOverride,
      jsonMapping = rawJsonMapping,
      jobRunId = jobRunId,
      raw = resolved
    ))
  }

  def parseArgs(sysArgs: Array[String], logger: GlueLogger): ResolvedArgs = {
    validateArgs(sysArgs) match {
      case Right(args) => args
      case Left(errs)  =>
        errs.foreach(logger.error)
        sys.exit(BAD_INPUT_EXIT_CODE)
    }
  }

  val RequiredConfNames: Seq[String] = Seq(
    "CassandraConnector.conf",
    "KeyspacesConnector.conf"
  )

  val RequiredJarNames: Seq[String] = Seq(
    "spark-cassandra-connector-assembly_2.12-3.5.1.jar",
    "aws-sigv4-auth-cassandra-java-driver-plugin-4.0.9.jar",
    "vavr-0.10.4.jar",
    "resilience4j-retry-1.7.1.jar",
    "resilience4j-core-1.7.1.jar"
  )

  /** Resolved bootstrap context returned by [[bootstrap]]. */
  final case class BootstrapContext(
      sparkContext: SparkContext,
      glueContext: GlueContext,
      sparkSession: SparkSession,
      s3client: AmazonS3,
      bucketName: String,
      prefix: ReconciliationPrefix
  )

  def parseBucketName(s3LandingZone: String): String = {
    val withoutScheme = s3LandingZone.replaceAll("s3://", "")
    val firstSlash = withoutScheme.indexOf('/')
    if (firstSlash < 0) withoutScheme else withoutScheme.substring(0, firstSlash)
  }


  final case class ReconciliationPrefix(uri: String, bucket: String, keyPrefix: String) {

    /** Bucket-relative key of the `report.json` collision sentinel. */
    def reportKey: String = s"${keyPrefix}report.json"
  }

  def splitS3Location(uriOrKey: String, defaultBucket: String): (String, String) = {
    val schemeStripped = uriOrKey.replaceFirst("^s3[an]?://", "")
    val hadScheme = schemeStripped != uriOrKey
    if (hadScheme) {
      val firstSlash = schemeStripped.indexOf('/')
      if (firstSlash < 0) (schemeStripped, "")
      else (schemeStripped.substring(0, firstSlash), schemeStripped.substring(firstSlash + 1))
    } else {
      (defaultBucket, schemeStripped.stripPrefix("/"))
    }
  }

  /**
   * Compute the effective Reconciliation_Prefix for a run.
   *
   * @param bucketName           landing-zone bucket (from [[parseBucketName]]).
   * @param sourceKs             source keyspace (default-prefix path segment).
   * @param sourceTbl            source table (default-prefix path segment).
   * @param runId                the validated `Run_ID` (Req 8.1 / 8.2).
   * @param outputPrefixOverride the raw `OUTPUT_PREFIX_OVERRIDE` value (may be
   *                             empty / whitespace-only, meaning "no override").
   * @return the resolved [[ReconciliationPrefix]].
   */
  def computeReconciliationPrefix(
      bucketName: String,
      sourceKs: String,
      sourceTbl: String,
      runId: String,
      outputPrefixOverride: String
  ): ReconciliationPrefix = {
    val override0 = Option(outputPrefixOverride).map(_.trim).getOrElse("")
    if (override0.nonEmpty) {
      val overrideNoTrailing = override0.replaceAll("/+$", "")
      val (bucket, basePrefix) = splitS3Location(overrideNoTrailing, bucketName)
      val keyPrefix =
        if (basePrefix.isEmpty) s"$runId/"
        else s"$basePrefix/$runId/"
      ReconciliationPrefix(s"s3://$bucket/$keyPrefix", bucket, keyPrefix)
    } else {
      val keyPrefix = s"$sourceKs/$sourceTbl/reconciliation/$runId/"
      ReconciliationPrefix(s"s3://$bucketName/$keyPrefix", bucketName, keyPrefix)
    }
  }

  def checkRunPrefixCollision(
      s3client: AmazonS3,
      prefix: ReconciliationPrefix,
      logger: GlueLogger
  ): Unit = {
    Try(s3client.doesObjectExist(prefix.bucket, prefix.reportKey)) match {
      case Success(true) =>
        logger.error(
          s"ERROR: RUN_ID_COLLISION -- a report.json already exists at the target " +
            s"reconciliation prefix ${prefix.uri}; refusing to overwrite an existing " +
            s"run. No artifact at that prefix was modified. Re-run with a distinct " +
            s"RUN_ID or OUTPUT_PREFIX_OVERRIDE."
        )
        sys.exit(RUN_ID_COLLISION_EXIT_CODE)
      case Success(false) =>
        logger.info(
          s"No existing report.json at ${prefix.uri}; proceeding with this run"
        )
      case Failure(t) =>
        logger.error(
          s"ERROR: BOOTSTRAP_FAIL -- failed to HEAD ${prefix.uri}report.json for the " +
            s"RUN_ID collision check: ${t.getClass.getName}: ${t.getMessage}"
        )
        sys.exit(BOOTSTRAP_FAIL_EXIT_CODE)
    }
  }

  def logAtLevel(logger: GlueLogger, level: String, message: String): Unit = {
    level.toUpperCase match {
      case "ERROR" => logger.error(message)
      case "WARN"  => logger.warn(message)
      case "INFO"  => logger.info(message)
      case "DEBUG" => logger.info(message)
      case _       => logger.info(message)
    }
  }

  def verifyArtifacts(
      s3client: AmazonS3,
      bucketName: String,
      requiredNames: Seq[String],
      logger: GlueLogger
  ): Unit = {
    val missing = scala.collection.mutable.ArrayBuffer.empty[String]
    requiredNames.foreach { name =>
      val key = s"artifacts/$name"
      val exists = Try(s3client.doesObjectExist(bucketName, key))
      exists match {
        case Success(true) =>
          ()
        case Success(false) =>
          val uri = s"s3://$bucketName/$key"
          logger.error(s"ERROR: required artifact $name is missing at $uri")
          missing += name
        case Failure(t) =>
          val uri = s"s3://$bucketName/$key"
          logger.error(
            s"ERROR: required artifact $name at $uri is unreadable: " +
              s"${t.getClass.getName}: ${t.getMessage}"
          )
          missing += name
      }
    }
    if (missing.nonEmpty) {
      logger.error(
        s"ERROR: ${missing.size} required artifact(s) missing or unreadable under " +
          s"s3://$bucketName/artifacts/; cannot start the Reconciliation_Job"
      )
      sys.exit(BOOTSTRAP_FAIL_EXIT_CODE)
    }
  }

  def bootstrap(
      parsed: ResolvedArgs,
      logger: GlueLogger
  ): BootstrapContext = {
    val bucketName = parseBucketName(parsed.s3LandingZone)
    val sparkContext = new SparkContext()
    val glueContext = new GlueContext(sparkContext)
    val sparkSession = glueContext.getSparkSession
    sparkContext.setLogLevel(parsed.logging)
    Job.init(parsed.jobName, glueContext, parsed.raw.asJava)

    val s3ClientConf = new ClientConfiguration()
      .withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    val s3client: AmazonS3 = AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(s3ClientConf)
      .build()

    val jsonMappingSummary = parsed.jsonMapping match {
      case "None" => "None"
      case raw    =>
        val previewLen = math.min(raw.length, 24)
        s"<base64 length=${raw.length} prefix='${raw.substring(0, previewLen)}...'>"
    }
    logAtLevel(
      logger,
      parsed.logging,
      "PrimaryKeyReconciliation resolved arguments: " +
        s"JOB_NAME=${parsed.jobName}, " +
        s"SOURCE_KS=${parsed.sourceKs}, SOURCE_TBL=${parsed.sourceTbl}, " +
        s"TARGET_KS=${parsed.targetKs}, TARGET_TBL=${parsed.targetTbl}, " +
        s"S3_LANDING_ZONE=${parsed.s3LandingZone}, bucketName=$bucketName, " +
        s"TOTAL_TILES=${parsed.totalTiles}, LOGGING=${parsed.logging}, " +
        s"RUN_ID=${parsed.runId}, KEEP_SNAPSHOTS=${parsed.keepSnapshots}, " +
        s"BIDIRECTIONAL=${parsed.bidirectional}, SAMPLE_SIZE=${parsed.sampleSize}, " +
        s"MAX_TARGET_RCUS=${parsed.maxTargetRcus}, " +
        s"OUTPUT_PREFIX_OVERRIDE='${parsed.outputPrefixOverride}', " +
        s"JSON_MAPPING=$jsonMappingSummary, JOB_RUN_ID=${parsed.jobRunId}"
    )

    verifyArtifacts(s3client, bucketName, RequiredConfNames ++ RequiredJarNames, logger)
    val prefix = computeReconciliationPrefix(
      bucketName = bucketName,
      sourceKs = parsed.sourceKs,
      sourceTbl = parsed.sourceTbl,
      runId = parsed.runId,
      outputPrefixOverride = parsed.outputPrefixOverride
    )
    logAtLevel(
      logger,
      parsed.logging,
      s"Effective Reconciliation_Prefix: ${prefix.uri}"
    )
    checkRunPrefixCollision(s3client, prefix, logger)

    BootstrapContext(sparkContext, glueContext, sparkSession, s3client, bucketName, prefix)
  }

  final case class PreflightContext(
      sourceConn: CqlSession,
      targetConn: CqlSession,
      reconciled: SchemaReconciliation.Reconciled,
      mapping: EffectiveMapping
  )

  private val SourceLabel: String = "source"
  private val TargetLabel: String = "target"

  def preflightAndReconcile(
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      logger: GlueLogger
  ): PreflightContext = {
    val sourceConn: CqlSession =
      getDBConnection("CassandraConnector.conf", bootstrapped.bucketName, bootstrapped.s3client)
    val targetConn: CqlSession =
      getDBConnection("KeyspacesConnector.conf", bootstrapped.bucketName, bootstrapped.s3client)

    def runPreflight(side: String, conn: CqlSession, ks: String, tbl: String): Unit = {
      try {
        preFlightCheck(conn, ks, tbl, side, logger)
      } catch {
        case e: PreFlightCheckException =>
          logger.error(
            s"ERROR: PREFLIGHT_FAIL on the $side side: keyspace=$ks, table=$tbl, " +
              s"errorCode=${e.errorCode}, detail=${e.message}"
          )
          sys.exit(e.errorCode)
      }
    }
    runPreflight(SourceLabel, sourceConn, parsed.sourceKs, parsed.sourceTbl)
    runPreflight(TargetLabel, targetConn, parsed.targetKs, parsed.targetTbl)

    val ColumnTsNone: String = "None"

    val srcPartitionKeysRaw: Seq[Map[String, String]] =
      inferKeys(sourceConn, "partitionKeys", parsed.sourceKs, parsed.sourceTbl, ColumnTsNone)
    val srcPrimaryKeysRaw: Seq[Map[String, String]] =
      inferKeys(sourceConn, "primaryKeys", parsed.sourceKs, parsed.sourceTbl, ColumnTsNone)
    val trgPartitionKeysRaw: Seq[Map[String, String]] =
      inferKeys(targetConn, "partitionKeys", parsed.targetKs, parsed.targetTbl, ColumnTsNone)
    val trgPrimaryKeysRaw: Seq[Map[String, String]] =
      inferKeys(targetConn, "primaryKeys", parsed.targetKs, parsed.targetTbl, ColumnTsNone)
    val srcPartitionKeys: Seq[(String, String)] =
      SchemaReconciliation.flattenInferKeys(srcPartitionKeysRaw)
    val srcPrimaryKeys: Seq[(String, String)] =
      SchemaReconciliation.flattenInferKeys(srcPrimaryKeysRaw)
    val trgPartitionKeys: Seq[(String, String)] =
      SchemaReconciliation.flattenInferKeys(trgPartitionKeysRaw)
    val trgPrimaryKeys: Seq[(String, String)] =
      SchemaReconciliation.flattenInferKeys(trgPrimaryKeysRaw)
    val srcClusteringKeysForLog: Seq[(String, String)] =
      srcPrimaryKeys.drop(srcPartitionKeys.size)
    val trgClusteringKeysForLog: Seq[(String, String)] =
      trgPrimaryKeys.drop(trgPartitionKeys.size)

    val mapping: EffectiveMapping =
      try JsonMappingResolver.resolve(parsed.jsonMapping, srcPrimaryKeys, logger)
      catch {
        case e: JsonMappingException =>
          logger.error(
            s"ERROR: ${e.getClass.getName}: ${e.getMessage}"
          )
          sys.exit(JSON_MAPPING_FAIL_EXIT_CODE)
      }

    val effectivePartitionKeys: Seq[(String, String)] =
      mapping.effectiveSourcePks.take(srcPartitionKeys.size)
    val effectiveClusteringKeys: Seq[(String, String)] =
      mapping.effectiveSourcePks.drop(srcPartitionKeys.size)

    logger.info(
      s"$SourceLabel partitionKeyColumns=" +
        SchemaReconciliation.renderKeySeq(srcPartitionKeys) +
        s", $SourceLabel clusteringKeyColumns=" +
        SchemaReconciliation.renderKeySeq(srcClusteringKeysForLog)
    )
    logger.info(
      s"$TargetLabel partitionKeyColumns=" +
        SchemaReconciliation.renderKeySeq(trgPartitionKeys) +
        s", $TargetLabel clusteringKeyColumns=" +
        SchemaReconciliation.renderKeySeq(trgClusteringKeysForLog)
    )
    logger.info(
      "Effective_Source_PK_Columns: partitionKeyColumns=" +
        SchemaReconciliation.renderKeySeq(effectivePartitionKeys) +
        ", clusteringKeyColumns=" +
        SchemaReconciliation.renderKeySeq(effectiveClusteringKeys)
    )

    // Step 6 -- schema reconciliation (Req 2.8, 4.5, 4.6, 4.7).
    val outcome = SchemaReconciliation.reconcile(
      srcPartitionKeys = srcPartitionKeys,
      srcPrimaryKeys = srcPrimaryKeys,
      trgPartitionKeys = trgPartitionKeys,
      trgPrimaryKeys = trgPrimaryKeys,
      mapping = mapping
    )

    outcome match {
      case r: SchemaReconciliation.Reconciled =>
        logger.info(
          s"Schema reconciled: partition-key and clustering-key " +
            s"(name, type) sequences are equal between source and target " +
            s"under Effective_Source_PK_Columns"
        )
        PreflightContext(sourceConn, targetConn, r, mapping)

      case SchemaReconciliation.EmptyPrimaryKeys(side) =>
        logger.error(
          s"ERROR: inferKeys returned an empty primaryKeys sequence for the $side " +
            s"(${if (side == SourceLabel) s"${parsed.sourceKs}.${parsed.sourceTbl}" else s"${parsed.targetKs}.${parsed.targetTbl}"}); " +
            s"cannot start the Reconciliation_Job"
        )
        sys.exit(BAD_INPUT_EXIT_CODE)

      case SchemaReconciliation.PartitionKeyMismatch(diffs) =>
        diffs.foreach { d =>
          logger.error(s"ERROR: partition-key mismatch: $d")
        }
        logger.error(
          s"ERROR: PARTITION_KEY_MISMATCH -- the post-transform source partition-key " +
            s"sequence is not (name, type)-equal to the target partition-key " +
            s"sequence in declared order"
        )
        sys.exit(PARTITION_KEY_MISMATCH_EXIT_CODE)

      case SchemaReconciliation.ClusteringKeyMismatch(diffs) =>
        diffs.foreach { d =>
          logger.error(s"ERROR: clustering-key mismatch: $d")
        }
        logger.error(
          s"ERROR: CLUSTERING_KEY_MISMATCH (policy=strict-fail) -- the post-transform " +
            s"source clustering-key sequence is not (name, type)-equal to the target " +
            s"clustering-key sequence in declared order"
        )
        sys.exit(CLUSTERING_KEY_MISMATCH_EXIT_CODE)
    }
  }

  private val IsoUtcMillis: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  def isoUtcMillis(instant: Instant): String = IsoUtcMillis.format(instant)

  def schemaFingerprint(
      partitionKeys: Seq[(String, String)],
      clusteringKeys: Seq[(String, String)]
  ): String = {
    val joined = (partitionKeys ++ clusteringKeys)
      .map { case (n, t) => s"$n:$t" }
      .mkString("|")
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(joined.getBytes(StandardCharsets.UTF_8))
    digest.map("%02x".format(_)).mkString
  }

  /**
   * Best-effort delete of every S3 object under the source-snapshot key
   * prefix `<prefix.keyPrefix>source/`, so a failed source write leaves no
   * partial Source_Snapshot behind (Req 14.1). Failures here are logged at
   * WARN and swallowed — the subsequent partial-report write is the
   * operator-visible signal, and the prefix is overwritten on the next
   * Glue retry of the same `Run_ID` (Req 5.10) regardless.
   */
  def cleanupSourceSnapshot(
      s3client: AmazonS3,
      prefix: ReconciliationPrefix,
      logger: GlueLogger
  ): Unit = {
    val sourceKeyPrefix = s"${prefix.keyPrefix}source/"
    Try {
      var listing = s3client.listObjectsV2(
        new ListObjectsV2Request().withBucketName(prefix.bucket).withPrefix(sourceKeyPrefix)
      )
      listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(prefix.bucket, obj.getKey))
      while (listing.isTruncated) {
        listing = s3client.listObjectsV2(
          new ListObjectsV2Request()
            .withBucketName(prefix.bucket)
            .withPrefix(sourceKeyPrefix)
            .withContinuationToken(listing.getNextContinuationToken)
        )
        listing.getObjectSummaries.asScala.foreach(obj => s3client.deleteObject(prefix.bucket, obj.getKey))
      }
    } match {
      case Success(_) =>
        logger.info(
          s"Removed any partial Source_Snapshot under s3://${prefix.bucket}/$sourceKeyPrefix"
        )
      case Failure(t) =>
        logger.warn(
          s"WARN: best-effort cleanup of partial Source_Snapshot under " +
            s"s3://${prefix.bucket}/$sourceKeyPrefix failed: " +
            s"${t.getClass.getName}: ${t.getMessage}"
        )
    }
  }

  def writePartialReport(
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: PreflightContext,
      phase: String,
      runStartedAt: Instant,
      t: Throwable
  ): Unit = {
    val reconciled = preflighted.reconciled
    val resolvedJsonMapping: Option[JsonMapping] =
      if (parsed.jsonMapping == null || parsed.jsonMapping == DefaultJsonMapping) None
      else Some(reconstructJsonMapping(preflighted.mapping))

    val failureMessage = Option(t.getMessage).getOrElse(t.toString)

    ReportWriter.write(ReportInputs(
      prefix = bootstrapped.prefix.uri,
      runId = parsed.runId,
      runStartedAt = isoUtcMillis(runStartedAt),
      runFinishedAt = isoUtcMillis(Instant.now()),
      srcKs = parsed.sourceKs, srcTbl = parsed.sourceTbl,
      trgKs = parsed.targetKs, trgTbl = parsed.targetTbl,
      partitionKeyColumns = reconciled.effectivePartitionKeys,
      clusteringKeyColumns = reconciled.effectiveClusteringKeys,
      schemaFingerprint =
        schemaFingerprint(reconciled.effectivePartitionKeys, reconciled.effectiveClusteringKeys),
      totalTiles = parsed.totalTiles,
      diff = None,
      status = "partial",
      phase = Some(phase),
      failure = Some(FailureBlock(t.getClass.getName, failureMessage)),
      jsonMapping = resolvedJsonMapping,
      sourceSnapshotPath = s"${bootstrapped.prefix.uri}source/",
      targetSnapshotPath = s"${bootstrapped.prefix.uri}target/",
      bidirectional = parsed.bidirectional,
      loggingLevel = parsed.logging
    ))
  }

  def reconstructJsonMapping(mapping: EffectiveMapping): JsonMapping = {
    val txExprs: List[TransformExpression] = mapping.pkTransforms.map { t =>
      TransformExpression(
        columnName = t.columnName,
        rule = t.rule,
        alias = t.alias.getOrElse(""),
        keepSource = t.keepSource
      )
    }.toList
    JsonMapping(
      keyspaces = Keyspaces(
        transformation = Transformation(
          enabled = true,
          filterExpression = mapping.filterExpr.getOrElse(""),
          transformExpressions = txExprs
        )
      )
    )
  }

  def phaseExitCode(phase: String): Int = phase match {
    case "preflight"   => PREFLIGHT_FAIL_EXIT_CODE
    case "source_scan" => SOURCE_SCAN_FAIL_EXIT_CODE
    case "target_scan" => TARGET_SCAN_FAIL_EXIT_CODE
    case "diff"        => DIFF_WRITE_FAIL_EXIT_CODE
    case "report"      => REPORT_WRITE_FAIL_EXIT_CODE
    case _             => BAD_INPUT_EXIT_CODE
  }

  private def maybeWritePartialReport(
      phase: String,
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: Option[PreflightContext],
      runStartedAt: Instant,
      t: Throwable,
      logger: GlueLogger
  ): Unit = phase match {
    case "source_scan" =>
      preflighted.foreach { p =>
        cleanupSourceSnapshot(bootstrapped.s3client, bootstrapped.prefix, logger)
        writePartialReport(parsed, bootstrapped, p, "source_scan", runStartedAt, t)
      }
    case "target_scan" =>
      preflighted.foreach(p =>
        writePartialReport(parsed, bootstrapped, p, "target_scan", runStartedAt, t)
      )
    case "diff" =>
      preflighted.foreach(p =>
        writePartialReport(parsed, bootstrapped, p, "diff", runStartedAt, t)
      )
    case _ =>
      ()
  }

  def runPhase[T](
      phase: String,
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: Option[PreflightContext],
      runStartedAt: Instant,
      logger: GlueLogger
  )(thunk: => T): T = {
    try {
      thunk
    } catch {
      case NonFatal(t) =>
        val (failClass, detail) = phase match {
          case "preflight" =>
            ("PREFLIGHT_FAIL", "the preflight / schema-reconciliation phase failed")
          case "source_scan" =>
            ("SOURCE_SCAN_FAIL",
              s"the source scan or Source_Snapshot write failed for " +
                s"${parsed.sourceKs}.${parsed.sourceTbl}")
          case "target_scan" =>
            ("TARGET_SCAN_FAIL",
              s"the target scan or Target_Snapshot write failed for " +
                s"${parsed.targetKs}.${parsed.targetTbl}")
          case "diff" =>
            ("DIFF_WRITE_FAIL",
              s"the diff phase failed for ${parsed.sourceKs}.${parsed.sourceTbl} vs " +
                s"${parsed.targetKs}.${parsed.targetTbl}")
          case "report" =>
            ("REPORT_WRITE_FAIL", "the report phase failed")
          case other =>
            (other.toUpperCase, s"the $other phase failed")
        }
        logger.error(
          s"ERROR: $failClass -- $detail: ${t.getClass.getName}: ${t.getMessage}"
        )
        maybeWritePartialReport(phase, parsed, bootstrapped, preflighted, runStartedAt, t, logger)
        sys.exit(phaseExitCode(phase))
    }
  }

  def runSourceScanPhase(
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: PreflightContext,
      runStartedAt: Instant,
      logger: GlueLogger
  ): Unit =
    runPhase("source_scan", parsed, bootstrapped, Some(preflighted), runStartedAt, logger) {
      val sourceDf = SourceScanner.scan(
        spark = bootstrapped.sparkSession,
        srcConnConfName = "CassandraConnector.conf",
        srcKs = parsed.sourceKs,
        srcTbl = parsed.sourceTbl,
        mapping = preflighted.mapping,
        totalTiles = parsed.totalTiles
      )

      val sourcePath = s"${bootstrapped.prefix.uri}source/"
      logger.info(
        s"SourceScanner: writing Source_Snapshot to $sourcePath " +
          s"(mode=overwrite, maxRecordsPerFile=$KEYS_PER_PARQUET_FILE)"
      )
      sourceDf.write
        .mode("overwrite")
        .option("maxRecordsPerFile", KEYS_PER_PARQUET_FILE)
        .parquet(sourcePath)
      logger.info(s"SourceScanner: Source_Snapshot write to $sourcePath completed")
    }

  def runTargetScanPhase(
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: PreflightContext,
      runStartedAt: Instant,
      logger: GlueLogger
  ): Unit = {
    val targetPks: Seq[(String, String)] =
      preflighted.reconciled.targetPartitionKeys ++ preflighted.reconciled.targetClusteringKeys

    val targetScanStartNanos: Long = System.nanoTime()

    runPhase("target_scan", parsed, bootstrapped, Some(preflighted), runStartedAt, logger) {
      val targetDf = TargetScanner.scan(
        spark = bootstrapped.sparkSession,
        trgConnConfName = "KeyspacesConnector.conf",
        trgKs = parsed.targetKs,
        trgTbl = parsed.targetTbl,
        targetPks = targetPks,
        totalTiles = parsed.totalTiles,
        maxTargetRcus = parsed.maxTargetRcus
      )

      val targetPath = s"${bootstrapped.prefix.uri}target/"
      logger.info(
        s"TargetScanner: writing Target_Snapshot to $targetPath " +
          s"(mode=overwrite, maxRecordsPerFile=$KEYS_PER_PARQUET_FILE)"
      )

      targetDf.cache()

      targetDf.write
        .mode("overwrite")
        .option("maxRecordsPerFile", KEYS_PER_PARQUET_FILE)
        .parquet(targetPath)

      val targetRowCount: Long = targetDf.count()
      val targetScanWallSeconds: Long =
        (System.nanoTime() - targetScanStartNanos) / 1000000000L
      logger.info(
        s"TargetScanner: Target_Snapshot write to $targetPath completed; " +
          s"targetRowCount=$targetRowCount, targetScannerWallSeconds=$targetScanWallSeconds"
      )

      targetDf.unpersist()
    }
  }

  def runDiffAndReportPhase(
      parsed: ResolvedArgs,
      bootstrapped: BootstrapContext,
      preflighted: PreflightContext,
      runStartedAt: Instant,
      logger: GlueLogger
  ): Unit = {
    val reconciled = preflighted.reconciled
    val effectivePkCols: Seq[String] =
      (reconciled.effectivePartitionKeys ++ reconciled.effectiveClusteringKeys).map(_._1)

    val sourcePath = s"${bootstrapped.prefix.uri}source/"
    val targetPath = s"${bootstrapped.prefix.uri}target/"
    val diffResult: DiffResult =
      runPhase("diff", parsed, bootstrapped, Some(preflighted), runStartedAt, logger) {
        val srcSnapshotDf = bootstrapped.sparkSession.read.parquet(sourcePath)
        val trgSnapshotDf = bootstrapped.sparkSession.read.parquet(targetPath)
        DiffEngine.compute(
          spark = bootstrapped.sparkSession,
          srcSnapshotDf = srcSnapshotDf,
          trgSnapshotDf = trgSnapshotDf,
          effectivePkCols = effectivePkCols,
          bidirectional = parsed.bidirectional,
          sampleSize = parsed.sampleSize,
          prefix = bootstrapped.prefix.uri
        )
      }

    runPhase("report", parsed, bootstrapped, Some(preflighted), runStartedAt, logger) {
      val resolvedJsonMapping: Option[JsonMapping] =
        if (parsed.jsonMapping == null || parsed.jsonMapping == DefaultJsonMapping) None
        else Some(reconstructJsonMapping(preflighted.mapping))

      ReportWriter.write(ReportInputs(
        prefix = bootstrapped.prefix.uri,
        runId = parsed.runId,
        runStartedAt = isoUtcMillis(runStartedAt),
        runFinishedAt = isoUtcMillis(Instant.now()),
        srcKs = parsed.sourceKs, srcTbl = parsed.sourceTbl,
        trgKs = parsed.targetKs, trgTbl = parsed.targetTbl,
        partitionKeyColumns = reconciled.effectivePartitionKeys,
        clusteringKeyColumns = reconciled.effectiveClusteringKeys,
        schemaFingerprint =
          schemaFingerprint(reconciled.effectivePartitionKeys, reconciled.effectiveClusteringKeys),
        totalTiles = parsed.totalTiles,
        diff = Some(diffResult),
        status = "success",
        phase = None,
        failure = None,
        jsonMapping = resolvedJsonMapping,
        sourceSnapshotPath = sourcePath,
        targetSnapshotPath = targetPath,
        bidirectional = parsed.bidirectional,
        loggingLevel = parsed.logging
      ))
    }
  }

  def main(args: Array[String]): Unit = {
    val logger = new GlueLogger
    val runStartedAt = Instant.now()
    val parsed = parseArgs(args, logger)
    val bootstrapped = bootstrap(parsed, logger)
    val preflighted = runPhase("preflight", parsed, bootstrapped, None, runStartedAt, logger) {
      preflightAndReconcile(parsed, bootstrapped, logger)
    }
    runSourceScanPhase(parsed, bootstrapped, preflighted, runStartedAt, logger)
    runTargetScanPhase(parsed, bootstrapped, preflighted, runStartedAt, logger)
    runDiffAndReportPhase(parsed, bootstrapped, preflighted, runStartedAt, logger)

    logger.info(
      s"PrimaryKeyReconciliation: run ${parsed.runId} completed successfully; " +
        s"artifacts under ${bootstrapped.prefix.uri}"
    )
  }
}
