/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake
package loader

import scala.util.control.NonFatal
import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import ast._
import ast.Select.Substring
import core._
import connection._
import ProcessManifest._
import loader.connection.Connection
import SnowflakeDatatype.{Char,Varchar}

object Loader {

  /** Check that necessary Snowflake entities are available before actual load */
  def preliminaryChecks[C](db: Connection[C], connection: C, schemaName: String, stageName: String, stageUrl: String, warehouseName: String): ValidatedNel[String, Unit] = {
    val schema = if (db.executeAndCountRows(connection, Show.ShowSchemas(Some(schemaName))) < 1)
      s"Schema $schemaName does not exist".invalidNel
    else ().validNel
    val stage = if (db.executeAndCountRows(connection, Show.ShowStages(Some(stageName), Some(schemaName))) < 1)
      s"Stage $stageName does not exist".invalidNel
    else ().validNel
    val snowflakeUrl = db.executeAndReturnResult(connection, Show.ShowStages(Some(stageName), Some(schemaName))).head("url").toString
    val stageUrlMatch = if (snowflakeUrl != stageUrl)
      s"Stage URL $stageUrl does not match specified in Snowflake $snowflakeUrl".invalidNel
    else ().validNel
    val table = if (db.executeAndCountRows(connection, Show.ShowTables(Some(Defaults.Table), Some(schemaName))) < 1)
      s"Table ${Defaults.Table} does not exist".invalidNel
    else ().validNel
    val fileFormat = if (db.executeAndCountRows(connection, Show.ShowFileFormats(Some(Defaults.FileFormat), Some(schemaName))) < 1)
      s"File format ${Defaults.FileFormat} does not exist".invalidNel
    else ().validNel
    val warehouse = if (db.executeAndCountRows(connection, Show.ShowWarehouses(Some(warehouseName))) < 1)
      s"Warehouse $warehouseName does not exist".invalidNel
    else ().validNel

    (schema, stage, stageUrlMatch, table, fileFormat, warehouse).map6 { (_: Unit, _: Unit, _: Unit, _: Unit, _: Unit, _: Unit) => () }
  }

  def exec[C](db: Connection[C], connection: C, manifest: ProcessManifest.Loader, config: Config): Unit = {
    val state = manifest.scan(config.manifest).map(SnowflakeState.getState) match {
      case Right(s) => s
      case Left(error) =>
        System.err.println(error)
        sys.exit(1)
    }

    preliminaryChecks(db, connection, config.schema, config.stage, config.stageUrl.toString, config.warehouse) match {
      case Validated.Valid(()) => println("Preliminary checks passed")
      case Validated.Invalid(errors) =>
        val message = s"Preliminary checks failed. ${errors.toList.mkString(", ")}"
        System.err.println(message)
        sys.exit(1)
    }

    if (state.foldersToLoad.isEmpty) {
      println("Nothing to load, exiting...")
      sys.exit(0)
    } else {
      db.execute(connection, UseWarehouse(config.warehouse))
      try {
        db.execute(connection, AlterWarehouse.Resume(config.warehouse))
        println(s"Warehouse ${config.warehouse} resumed")
      } catch {
        case _: net.snowflake.client.jdbc.SnowflakeSQLException =>
          println(s"Warehouse ${config.warehouse} already resumed")
      }

      // Check that folders are located inside stage (#62)
      state.foldersToLoad.foreach { folder =>
        if (!folder.folderToLoad.savedTo.isSubdirOf(config.stageUrl)) {
          val message = s"Folder ${folder.folderToLoad.savedTo} is located outside of stage ${config.stageUrl}, skipping..."
          System.err.println(message)
          sys.exit(1)
        }
      }

      // Add each folder in transaction
      state.foldersToLoad.foreach { folder =>
        val transactionName = s"snowplow_${folder.folderToLoad.runIdFolder}".replaceAll("=", "_").replaceAll("-", "_")
        db.startTransaction(connection, Some(transactionName))
        try {
          addColumns(db, connection, config.schema, folder)
          loadFolder(db, connection, config, folder.folderToLoad)
        } catch {
          case e: Throwable =>
            val message = if (folder.newColumns.isEmpty)
              s"Error during ${folder.folderToLoad.runId} load. ${e.getMessage}\n" +
                s"No new columns were added, safe to rerun."
            else
              s"Error during ${folder.folderToLoad.runId} load. ${e.getMessage}\nTrying to rollback"
            db.rollbackTransaction(connection)
            System.err.println(message)
            println("Failure, exiting...")
            sys.exit(1)
        }
        manifest.markLoaded(config.manifest, folder.folderToLoad.runId)
        db.commitTransaction(connection)
      }
    }
  }

  /** Scan state from processing manifest, extract not-loaded folders and lot each of them */
  def run(config: Config.CliLoaderConfiguration): Unit = {
    if (config.dryRun) {
      ProcessManifest.buildDynamoDb(config.loaderConfig.awsRegion)
      exec(DryRun, new DryRun(), DryRunProcessingManifest, config.loaderConfig)
    } else {
      ProcessManifest.buildDynamoDb(config.loaderConfig.awsRegion)
      val manifest = AwsLoaderProcessingManifest
      val connection = Jdbc.getConnection(config.loaderConfig)
      exec(Jdbc, connection, manifest, config.loaderConfig)
    }
    println("Success. Exiting...")
  }

  /** Create INSERT statement to load Processed Run Id */
  def getInsertStatement(config: Config, folder: RunId.ProcessedRunId): Insert = {
    val tableColumns = getColumns(folder.shredTypes)
    val castedColumns = tableColumns.map {
      // Truncate columns
      case ("refr_term", Varchar(Some(_))) => Select.CastedColumn(Defaults.TempTableColumn, "refr_term", Varchar(None), Some(Substring(1, 255)))
      // Remove VARCHAR precision (#54)
      case (name, Varchar(Some(_))) => Select.CastedColumn(Defaults.TempTableColumn, name, Varchar(None))
      case (name, Char(_)) => Select.CastedColumn(Defaults.TempTableColumn, name, Varchar(None))
      case (name, dataType) => Select.CastedColumn(Defaults.TempTableColumn, name, dataType)
    }
    val tempTable = getTempTable(folder.runIdFolder, config.schema)
    val source = Select(castedColumns, tempTable.schema, tempTable.name)
    Insert.InsertQuery(config.schema, Defaults.Table, tableColumns.map(_._1), source)
  }

  /**
    * Build CREATE TABLE statement for temporary table
    * @param runId arbitrary identifier
    * @param dbSchema Snowflake DB Schema
    * @return SQL statement AST
    */
  def getTempTable(runId: String, dbSchema: String): CreateTable = {
    val tempTableName = "snowplow_tmp_" + runId.replace('=', '_').replace('-', '_')
    val enrichedColumn = Column(Defaults.TempTableColumn, SnowflakeDatatype.JsonObject, notNull = true)
    CreateTable(dbSchema, tempTableName, List(enrichedColumn), None, temporary = true)
  }

  /**
    * Get list of pairs with column name and datatype based on canonical columns and new shred types
    * @param shredTypes shred types discovered in particular run id
    * @return pairs of string, ready to be used in statement, e.g. (app_id, VARCHAR(255))
    */
  def getColumns(shredTypes: List[String]): List[(String, SnowflakeDatatype)] = {
    val atomicColumns = AtomicDef.columns.map(c => (c.name, c.dataType))
    val shredTypeColumns = shredTypes.map(getShredType).sequence match {
      case Right(types) => types
      case Left(e) => throw new RuntimeException(e)
    }
    atomicColumns ++ shredTypeColumns
  }

  /**
    * Create temporary table with single OBJECT column and load data from S3
    * @param connection JDBC connection
    * @param config loader configuration
    * @param tempTableCreateStatement SQL statement AST
    * @param runId directory in stage, where files reside
    */
  def loadTempTable[C](db: Connection[C], connection: C, config: Config, tempTableCreateStatement: CreateTable, runId: String): Unit = {
    val credentials = PasswordService.getLoadCredentials(config.auth) match {
      case Left(PasswordService.NoCredentials) => None
      case Left(PasswordService.CredentialsFailure(error)) => throw new RuntimeException(error)
      case Right(creds) => Some(creds)
    }

    val tempTableCopyStatement = CopyInto(
      tempTableCreateStatement.schema,
      tempTableCreateStatement.name,
      List(Defaults.TempTableColumn),
      CopyInto.From(config.schema, config.stage, runId),
      credentials,
      CopyInto.FileFormat(config.schema, Defaults.FileFormat),
      config.maxError.map(CopyInto.SkipFileNum),
      true)

    db.execute(connection, tempTableCreateStatement)
    db.execute(connection, tempTableCopyStatement)
  }

  /** Try to infer ARRAY/OBJECT type, based on column name */
  def getShredType(columnName: String): Either[String, (String, SnowflakeDatatype)] = {
    if (columnName.startsWith("contexts_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonArray)
    } else if (columnName.startsWith("unstruct_event_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonObject)
    } else {
      Left(s"Column [$columnName] is not a shredded type")
    }
  }

  // Actual DB IO actions

  /**
    * Execute loading statement for processed run id
    * @param connection JDBC connection
    * @param config load configuration
    * @param folder run id, extracted from manifest; processed, but not yet loaded
    */
  private def loadFolder[C](db: Connection[C], connection: C, config: Config, folder: RunId.ProcessedRunId): Unit = {
    val runId = folder.runIdFolder
    val tempTable = getTempTable(runId, config.schema)
    loadTempTable(db, connection, config, tempTable, runId)

    val loadStatement = getInsertStatement(config, folder)
    db.execute(connection, loadStatement)
    println(s"Folder [$runId] from stage [${config.stage}] has been loaded")
  }

  /** Add new column with VARIANT type to events table */
  private def addShredType[C](db: Connection[C], connection: C, schemaName: String, columnName: String): Unit = {
    val shredType = getShredType(columnName) match {
      case Right((_, datatype)) => datatype
      case Left(error) => throw new RuntimeException(error)
    }
    db.execute(connection, AlterTable.AddColumn(schemaName, Defaults.Table, columnName, shredType))
    println(s"New column [$columnName] has been added")
  }

  /**
    * Add columns for shred types that were encountered for particular folder to events table (in alphabetical order)
    * Print human-readable error and exit in case of error
    * @param db connection interpreter
    * @param connection JDBC connection
    * @param schema Snowflake Schema for events table
    * @param folder folder parsed from manifest
    */
  private def addColumns[C](db: Connection[C], connection: C, schema: String, folder: SnowflakeState.FolderToLoad): Unit = {
    val columns = folder.newColumns.toList.sorted

    val _ = columns.foldLeft(List.empty[String]) { (created, current) =>
      try {
        addShredType(db, connection, schema, current)
        current :: created
      } catch {
        case NonFatal(e) =>
          val message = s"${e.getMessage}\nFollowing columns were added during load-preparation and must be manually dropped in order to restore state: ${created.mkString(", ")}"
          throw new RuntimeException(message)
      }
    }
    ()
  }
}
