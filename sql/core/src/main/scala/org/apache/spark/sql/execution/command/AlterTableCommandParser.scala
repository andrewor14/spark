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

package org.apache.spark.sql.execution.command

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, SortDirection}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType


/**
 * Helper object to parse alter table commands.
 */
object AlterTableCommandParser {
  import ParserUtils._

  /**
   * Parse the given node assuming it is an alter table command.
   */
  def parse(v1: ASTNode): LogicalPlan = {
    v1.children match {
      case (tabName @ Token("TOK_TABNAME", _)) :: restNodes =>
        val tableIdent = extractTableIdent(tabName)
        val partSpec = getClauseOption("TOK_PARTSPEC", v1.children).map(parsePartitionSpec)
        matchAlterTableCommands(v1, restNodes, tableIdent, partSpec)
      case _ =>
        parseFailed("Could not parse ALTER TABLE command: ", v1)
    }
  }

  private def cleanAndUnquoteString(s: String): String = {
    cleanIdentifier(unquoteString(s))
  }

  private def parseFailed(msg: String, node: ASTNode): Nothing = {
    throw new AnalysisException(s"$msg: '${node.source}")
  }

  /**
   * Extract partition spec from the given [[ASTNode]] as a map, assuming it exists.
   *
   * Expected format:
   *   +- TOK_PARTSPEC
   *      :- TOK_PARTVAL
   *      :  :- dt
   *      :  +- '2008-08-08'
   *      +- TOK_PARTVAL
   *         :-country
   *         +- 'us'
   */
  private def parsePartitionSpec(node: ASTNode): Map[String, String] = {
    node match {
      case Token("TOK_PARTSPEC", partitions) =>
        partitions.map {
          case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
            (cleanAndUnquoteString(ident.text), cleanAndUnquoteString(constant.text))
          case Token("TOK_PARTVAL", ident :: Nil) =>
            (cleanAndUnquoteString(ident.text), null)
        }.toMap
      case _ =>
        parseFailed(s"Expected partition spec in ALTER TABLE command", node)
    }
  }

  /**
   * Extract table properties from the given [[ASTNode]] as a map, assuming it exists.
   *
   * Expected format:
   *   +- TOK_TABLEPROPERTIES
   *      +- TOK_TABLEPROPLIST
   *         :- TOK_TABLEPROPERTY
   *         :  :- 'test'
   *         :  +- 'value'
   *         +- TOK_TABLEPROPERTY
   *            :- 'comment'
   *            +- 'new_comment'
   */
  private def extractTableProps(node: ASTNode): Map[String, String] = {
    node match {
      case Token("TOK_TABLEPROPERTIES", propsList) =>
        propsList.flatMap {
          case Token("TOK_TABLEPROPLIST", props) =>
            props.map { case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
              val k = cleanAndUnquoteString(key.text)
              val v = value match {
                case Token("TOK_NULL", Nil) => null
                case _ => cleanAndUnquoteString(value.text)
              }
              (k, v)
            }
        }.toMap
      case _ =>
        parseFailed("Expected table properties in ALTER TABLE command", node)
    }
  }

  /**
   * Parse an alter table command from a [[ASTNode]] into a [[LogicalPlan]].
   * This follows https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL.
   */
  // TODO: This method is massive. Break it down.
  private def matchAlterTableCommands(
      node: ASTNode,
      nodes: Seq[ASTNode],
      tableIdent: TableIdentifier,
      partition: Option[Map[String, String]]): LogicalPlan = {
    nodes match {
      // ALTER TABLE table_name RENAME TO new_table_name;
      case rename @ Token("TOK_ALTERTABLE_RENAME", renameArgs) :: _ =>
        val tableNameClause = getClause("TOK_TABNAME", renameArgs)
        val newTableIdent = extractTableIdent(tableNameClause)
        AlterTableRename(tableIdent, newTableIdent)(node.source)

      // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
      case Token("TOK_ALTERTABLE_PROPERTIES", args) :: _ =>
        val properties = extractTableProps(args.head)
        AlterTableSetProperties(tableIdent, properties)(node.source)

      // ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'key');
      case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) :: _ =>
        val properties = extractTableProps(args.head)
        val ifExists = getClauseOption("TOK_IFEXISTS", args).isDefined
        AlterTableUnsetProperties(tableIdent, properties, ifExists)(node.source)

      // ALTER TABLE table_name [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
      case Token("TOK_ALTERTABLE_SERIALIZER", Token(serdeClassName, Nil) :: serdeArgs) :: _ =>
        AlterTableSerDeProperties(
          tableIdent,
          Some(cleanAndUnquoteString(serdeClassName)),
          serdeArgs.headOption.map(extractTableProps),
          partition)(node.source)

      // ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
      case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) :: _ =>
        AlterTableSerDeProperties(
          tableIdent,
          None,
          Some(extractTableProps(args.head)),
          partition)(node.source)

      // ALTER TABLE table_name CLUSTERED BY (col, ...) [SORTED BY (col, ...)] INTO n BUCKETS;
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_ALTERTABLE_BUCKETS", b) :: Nil) :: _ =>
        val clusterCols: Seq[String] = b.head match {
          case Token("TOK_TABCOLNAME", children) => children.map(_.text)
          case _ => parseFailed(s"Invalid ALTER TABLE command", node)
        }
        // If sort columns are specified, num buckets should be the third arg.
        // If sort columns are not specified, num buckets should be the second arg.
        val (sortCols: Seq[String], sortDirections: Seq[SortDirection], numBuckets: Int) = {
          b.tail match {
            case Token("TOK_TABCOLNAME", children) :: numBucketsNode :: Nil =>
              val (cols, directions) = children.map {
                case Token("TOK_TABSORTCOLNAMEASC", Token(col, Nil) :: Nil) => (col, Ascending)
                case Token("TOK_TABSORTCOLNAMEDESC", Token(col, Nil) :: Nil) => (col, Descending)
              }.unzip
              (cols, directions, numBucketsNode.text.toInt)
            case numBucketsNode :: Nil =>
              (Nil, Nil, numBucketsNode.text.toInt)
            case _ =>
              parseFailed(s"Invalid ALTER TABLE command", node)
          }
        }
        AlterTableStorageProperties(
          tableIdent,
          BucketSpec(numBuckets, clusterCols, sortCols, sortDirections))(node.source)

      // ALTER TABLE table_name NOT CLUSTERED
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_NOT_CLUSTERED", Nil) :: Nil) :: _ =>
        AlterTableNotClustered(tableIdent)(node.source)

      // ALTER TABLE table_name NOT SORTED
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_NOT_SORTED", Nil) :: Nil) :: _ =>
        AlterTableNotSorted(tableIdent)(node.source)

      // ALTER TABLE table_name SKEWED BY (col1, col2)
      //   ON ((col1_value, col2_value) [, (col1_value, col2_value), ...])
      //   [STORED AS DIRECTORIES];
      case Token("TOK_ALTERTABLE_SKEWED",
          Token("TOK_TABLESKEWED",
          Token("TOK_TABCOLNAME", colNames) :: colValues :: rest) :: Nil) :: Nil =>
        // Example format:
        //
        //   +- TOK_ALTERTABLE_SKEWED
        //      :- TOK_TABLESKEWED
        //      :  :- TOK_TABCOLNAME
        //      :  :  :- dt
        //      :  :  +- country
        //      :- TOK_TABCOLVALUE_PAIR
        //      :  :- TOK_TABCOLVALUES
        //      :  :  :- TOK_TABCOLVALUE
        //      :  :  :  :- '2008-08-08'
        //      :  :  :  +- 'us'
        //      :  :- TOK_TABCOLVALUES
        //      :  :  :- TOK_TABCOLVALUE
        //      :  :  :  :- '2009-09-09'
        //      :  :  :  +- 'uk'
        //      +- TOK_STOREASDIR
        val names = colNames.map { n => cleanAndUnquoteString(n.text) }
        val values = colValues match {
          case Token("TOK_TABCOLVALUE", vals) =>
            Seq(vals.map { n => cleanAndUnquoteString(n.text) })
          case Token("TOK_TABCOLVALUE_PAIR", pairs) =>
            pairs.map {
              case Token("TOK_TABCOLVALUES", Token("TOK_TABCOLVALUE", vals) :: Nil) =>
                vals.map { n => cleanAndUnquoteString(n.text) }
              case _ =>
                parseFailed(s"Invalid ALTER TABLE command", node)
            }
          case _ =>
            parseFailed(s"Invalid ALTER TABLE command", node)
        }
        val storedAsDirs = rest match {
          case Token("TOK_STOREDASDIRS", Nil) :: Nil => true
          case _ => false
        }
        AlterTableSkewed(
          tableIdent,
          names,
          values,
          storedAsDirs)(node.source)

      // ALTER TABLE table_name NOT SKEWED
      case Token("TOK_ALTERTABLE_SKEWED", Nil) :: Nil =>
        AlterTableNotSkewed(tableIdent)(node.source)

      // ALTER TABLE table_name NOT STORED AS DIRECTORIES
      case Token("TOK_ALTERTABLE_SKEWED", Token("TOK_STOREDASDIRS", Nil) :: Nil) :: Nil =>
        AlterTableNotStoredAsDirs(tableIdent)(node.source)

      // ALTER TABLE table_name SET SKEWED LOCATION (col1="loc1" [, (col2, col3)="loc2", ...] );
      case Token("TOK_ALTERTABLE_SKEWED_LOCATION",
        Token("TOK_SKEWED_LOCATIONS",
        Token("TOK_SKEWED_LOCATION_LIST", locationMaps) :: Nil) :: Nil) :: _ =>
        // Expected format:
        //
        //   +- TOK_ALTERTABLE_SKEWED_LOCATION
        //      +- TOK_SKEWED_LOCATIONS
        //         +- TOK_SKEWED_LOCATION_LIST
        //            :- TOK_SKEWED_LOCATION_MAP
        //            :  :- 'col1'
        //            :  +- 'loc1'
        //            +- TOK_SKEWED_LOCATION_MAP
        //               :- TOK_TABCOLVALUES
        //               :  +- TOK_TABCOLVALUE
        //               :     :- 'col2'
        //               :     +- 'col3'
        //               +- 'loc2'
        val skewedMaps = locationMaps.flatMap {
          case Token("TOK_SKEWED_LOCATION_MAP", col :: loc :: Nil) =>
            col match {
              case Token(const, Nil) =>
                Seq((cleanAndUnquoteString(const), cleanAndUnquoteString(loc.text)))
              case Token("TOK_TABCOLVALUES", Token("TOK_TABCOLVALUE", keys) :: Nil) =>
                keys.map { k => (cleanAndUnquoteString(k.text), cleanAndUnquoteString(loc.text)) }
            }
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }.toMap
        AlterTableSkewedLocation(tableIdent, skewedMaps)(node.source)

      // TODO(andrew): clean up the rest of this!
      case Token("TOK_ALTERTABLE_ADDPARTS", addPartsArgs) :: _ =>
        val (allowExisting, parts) = addPartsArgs match {
          case Token("TOK_IFNOTEXISTS", Nil) :: others => (true, others)
          case _ => (false, addPartsArgs)
        }
        val partitions: ArrayBuffer[(Map[String, String], Option[String])] =
          new ArrayBuffer()
        var currentPart: Map[String, String] = null
        parts.map {
          case t @ Token("TOK_PARTSPEC", partArgs) =>
            if (currentPart != null) {
              partitions += ((currentPart, None))
            }
            currentPart = parsePartitionSpec(t)
          case Token("TOK_PARTITIONLOCATION", loc :: Nil) =>
            val location = unquoteString(loc.text)
            if (currentPart != null) {
              partitions += ((currentPart, Some(location)))
              currentPart = null
            } else {
              // We should not reach here
              throw new AnalysisException("Partition location must follow a partition spec.")
            }
        }
        if (currentPart != null) {
          partitions += ((currentPart, None))
        }
        AlterTableAddPartition(tableIdent, partitions, allowExisting)(node.source)

      case Token("TOK_ALTERTABLE_RENAMEPART", partArg :: Nil) :: _ =>
        val newPartition = parsePartitionSpec(partArg)
        AlterTableRenamePartition(tableIdent, partition.get, newPartition)(node.source)

      case Token("TOK_ALTERTABLE_EXCHANGEPARTITION",
      (p @ Token("TOK_PARTSPEC", _)) :: (t @ Token("TOK_TABNAME", _)) :: Nil) :: _ =>
        val partition = parsePartitionSpec(p)
        val fromTableIdent = extractTableIdent(t)
        AlterTableExchangePartition(tableIdent, fromTableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_DROPPARTS", args) :: _ =>
        val parts = args.collect {
          case Token("TOK_PARTSPEC", partitions) =>
            partitions.map {
              case Token("TOK_PARTVAL", ident :: op :: constant :: Nil) =>
                (cleanAndUnquoteString(ident.text),
                  op.text, cleanAndUnquoteString(constant.text))
            }
        }
        val allowExisting = getClauseOption("TOK_IFEXISTS", args).isDefined
        val purge = getClauseOption("PURGE", args)
        val replication = getClauseOption("TOK_REPLICATION", args).map {
          case Token("TOK_REPLICATION", replId :: metadata) =>
            (cleanAndUnquoteString(replId.text), metadata.nonEmpty)
        }
        AlterTableDropPartition(
          tableIdent,
          parts,
          allowExisting,
          purge.isDefined,
          replication)(node.source)

      case Token("TOK_ALTERTABLE_ARCHIVE", partArg :: Nil) :: _ =>
        val partition = parsePartitionSpec(partArg)
        AlterTableArchivePartition(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_UNARCHIVE", partArg :: Nil) :: _ =>
        val partition = parsePartitionSpec(partArg)
        AlterTableUnarchivePartition(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_FILEFORMAT", args) :: _ =>
        val Seq(fileFormat, genericFormat) =
          getClauses(Seq("TOK_TABLEFILEFORMAT", "TOK_FILEFORMAT_GENERIC"),
            args)
        val fFormat = fileFormat.map(_.children.map(n => cleanAndUnquoteString(n.text)))
        val gFormat = genericFormat.map(f => cleanAndUnquoteString(f.children(0).text))
        AlterTableSetFileFormat(tableIdent, partition, fFormat, gFormat)(node.source)

      case Token("TOK_ALTERTABLE_LOCATION", Token(loc, Nil) :: Nil) :: _ =>
        AlterTableSetLocation(tableIdent, partition, cleanAndUnquoteString(loc))(node.source)

      case Token("TOK_ALTERTABLE_TOUCH", args) :: _ =>
        val part = getClauseOption("TOK_PARTSPEC", args).map(parsePartitionSpec)
        AlterTableTouch(tableIdent, part)(node.source)

      case Token("TOK_ALTERTABLE_COMPACT", Token(compactType, Nil) :: Nil) :: _ =>
        AlterTableCompact(tableIdent, partition, cleanAndUnquoteString(compactType))(node.source)

      case Token("TOK_ALTERTABLE_MERGEFILES", _) :: _ =>
        AlterTableMerge(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_RENAMECOL", args) :: _ =>
        val oldName = args(0).text
        val newName = args(1).text
        val dataType = nodeToDataType(args(2))
        val afterPos =
          getClauseOption("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", args)
        val afterPosCol = afterPos.map { ap =>
          ap.children match {
            case Token(col, Nil) :: Nil => col
            case _ => null
          }
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        val comment = if (args.size > 3) {
          args(3) match {
            case Token(commentStr, Nil)
              if commentStr != "TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION" &&
                commentStr != "TOK_RESTRICT" && commentStr != "TOK_CASCADE" =>
              Some(cleanAndUnquoteString(commentStr))
            case _ =>
              None
          }
        } else {
          None
        }
        AlterTableChangeCol(
          tableIdent,
          partition,
          oldName,
          newName,
          dataType,
          comment,
          afterPos.isDefined,
          afterPosCol,
          restrict.isDefined,
          cascade.isDefined)(node.source)

      case Token("TOK_ALTERTABLE_ADDCOLS", args) :: _ =>
        val tableCols = getClause("TOK_TABCOLLIST", args)
        val columns = tableCols match {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        AlterTableAddCol(
          tableIdent,
          partition,
          columns,
          restrict.isDefined,
          cascade.isDefined)(node.source)

      case Token("TOK_ALTERTABLE_REPLACECOLS", args) :: _ =>
        val tableCols = getClause("TOK_TABCOLLIST", args)
        val columns = tableCols match {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        AlterTableReplaceCol(
          tableIdent,
          partition,
          columns,
          restrict.isDefined,
          cascade.isDefined)(node.source)

      case _ =>
        throw new AnalysisException(s"Unsupported ALTER TABLE command: '${node.source}'")
    }
  }

}
