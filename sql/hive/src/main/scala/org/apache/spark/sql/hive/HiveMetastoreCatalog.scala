/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.{CreateTableAsSelectLogicalPlan, CreateViewCommand, HiveNativeCommand}
import org.apache.spark.sql.execution.datasources.{Partition => _, _}
import org.apache.spark.sql.execution.datasources.parquet.{DefaultSource => ParquetDefaultSource, ParquetRelation}
import org.apache.spark.sql.hive.orc.{DefaultSource => OrcDefaultSource}
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types._


/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val conf = sparkSession.conf
  private val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
  private val client = sparkSession.sharedState.asInstanceOf[HiveSharedState].metadataHive

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String)

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  private def getQualifiedTableName(t: CatalogTable): QualifiedTableName = {
    QualifiedTableName(
      t.identifier.database.getOrElse(getCurrentDatabase).toLowerCase,
      t.identifier.table.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        def schemaStringFromParts: Option[String] = {
          table.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"spark.sql.sources.schema.part.$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        def getColumnNames(colType: String): Seq[String] = {
          table.properties.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").map {
            numCols => (0 until numCols.toInt).map { index =>
              table.properties.getOrElse(s"spark.sql.sources.schema.${colType}Col.$index",
                throw new AnalysisException(
                  s"Could not read $colType columns from the metastore because it is corrupted " +
                    s"(missing part $index of it, $numCols parts are expected)."))
            }
          }.getOrElse(Nil)
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString =
          table.properties.get("spark.sql.sources.schema").orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get datatypes of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = getColumnNames("part")

        val bucketSpec = table.properties.get("spark.sql.sources.schema.numBuckets").map { n =>
          BucketSpec(n.toInt, getColumnNames("bucket"), getColumnNames("sort"))
        }

        val options = table.storage.serdeProperties
        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = userSpecifiedSchema,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = table.properties("spark.sql.sources.provider"),
            options = options)

        LogicalRelation(
          dataSource.resolveRelation(),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(tableIdent)
  }

  def invalidateTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = client.getTable(qualifiedTableName.database, qualifiedTableName.name)

    if (table.properties.get("spark.sql.sources.provider").isDefined) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIRTUAL_VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None => SubqueryAlias(table.identifier.table, sparkSession.parseSql(viewText))
        case Some(aliasText) => SubqueryAlias(aliasText, sparkSession.parseSql(viewText))
      }
    } else {
      MetastoreRelation(
        qualifiedTableName.database, qualifiedTableName.name, alias)(table, client, sparkSession)
    }
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    cachedDataSourceTables.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val pathsInMetastore = metastoreRelation.catalogTable.storage.locationUri.toSeq
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                  PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
                }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              cachedDataSourceTables.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        cachedDataSourceTables.invalidate(tableIdentifier)
        None
    }
  }

  private def convertToLogicalRelation(
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)

      val cached = getCached(
        tableIdentifier,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        Some(partitionSpec))

      val hadoopFsRelation = cached.getOrElse {
        val paths = new Path(metastoreRelation.catalogTable.storage.locationUri.get) :: Nil
        val fileCatalog = new MetaStoreFileCatalog(sparkSession, paths, partitionSpec)

        val inferredSchema = if (fileType.equals("parquet")) {
          val inferredSchema =
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
          inferredSchema.map { inferred =>
            ParquetRelation.mergeMetastoreParquetSchema(metastoreSchema, inferred)
          }.getOrElse(metastoreSchema)
        } else {
          defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
        }

        val relation = HadoopFsRelation(
          sparkSession = sparkSession,
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = inferredSchema,
          bucketSpec = bucketSpec,
          fileFormat = defaultSource,
          options = options)

        val created = LogicalRelation(relation)
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      hadoopFsRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        None)
      val logicalRelation = cached.getOrElse {
        val created =
          LogicalRelation(
            DataSource(
              sparkSession = sparkSession,
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              bucketSpec = bucketSpec,
              options = options,
              className = fileType).resolveRelation())

        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      logicalRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
        sessionState.convertMetastoreParquet
    }

    private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new ParquetDefaultSource()
      val fileFormatClass = classOf[ParquetDefaultSource]

      val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
      val options = Map(
        ParquetRelation.MERGE_SCHEMA -> mergeSchema.toString,
        ParquetRelation.METASTORE_TABLE_NAME -> TableIdentifier(
          relation.tableName,
          Some(relation.databaseName)
        ).unquotedString
      )

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
          val parquetRelation = convertToParquetRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), parquetRelation)
      }
    }
  }

  /**
   * When scanning Metastore ORC tables, convert them to ORC data source relations
   * for better performance.
   */
  object OrcConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
        sessionState.convertMetastoreOrc
    }

    private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new OrcDefaultSource()
      val fileFormatClass = classOf[OrcDefaultSource]
      val options = Map[String, String]()

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
          val orcRelation = convertToOrcRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), orcRelation)
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p

      case CreateViewCommand(table, child, allowExisting, replace, sql) if !conf.nativeView =>
        HiveNativeCommand(sql)

      case p @ CreateTableAsSelectLogicalPlan(table, child, allowExisting) =>
        val schema = if (table.schema.nonEmpty) {
          table.schema
        } else {
          child.output.map { a =>
            CatalogColumn(a.name, a.dataType.catalogString, a.nullable)
          }
        }

        val desc = table.copy(schema = schema)

        if (sessionState.convertCTAS && table.storage.serde.isEmpty) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (table.identifier.database.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
                "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            TableIdentifier(desc.identifier.table),
            conf.defaultDataSourceName,
            temporary = false,
            Array.empty[String],
            bucketSpec = None,
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          val desc = if (table.storage.serde.isEmpty) {
            // add default serde
            table.withNewStorage(
              serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
          } else {
            table
          }

          val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

          execution.CreateTableAsSelect(
            desc.copy(identifier = TableIdentifier(tblName, Some(dbName))),
            child,
            allowExisting)
        }
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan)
      : LogicalPlan = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val numDynamicPartitions = p.partition.values.count(_.isEmpty)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions))
          .take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => left.sameType(right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 */
private[hive] class MetaStoreFileCatalog(
    sparkSession: SparkSession,
    paths: Seq[Path],
    partitionSpecFromHive: PartitionSpec)
  extends HDFSFileCatalog(
    sparkSession,
    Map.empty,
    paths,
    Some(partitionSpecFromHive.partitionColumns)) {

  override def getStatus(path: Path): Array[FileStatus] = {
    val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    fs.listStatus(path)
  }

  override def partitionSpec(): PartitionSpec = partitionSpecFromHive
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  val numDynamicPartitions = partition.values.count(_.isEmpty)

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions)

  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
  }
}
