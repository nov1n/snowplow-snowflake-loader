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
package com.snowplowanalytics.snowflake.loader

import org.specs2.Specification
import org.joda.time.DateTime

import ast.{Insert, Select, SnowflakeDatatype, Statement}

import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}
import com.snowplowanalytics.snowflake.loader
import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.loader.connection.Connection
import com.snowplowanalytics.snowflake.core.{ProcessManifest, RunId, Config}

class LoaderSpec extends Specification { def is = s2"""
  Parse context column name as ARRAY type $e1
  Parse unstruct event column name as OBJECT type $e2
  Fail to parse invalid column name $e3
  Fail with exception to get list of columns $e4
  Build valid INSERT statement $e5
  Look at output $e6
  """

  def e1 = {
    val columnName = "contexts_com_snowplowanalytics_snowplow_web_page_1"
    val expected = "contexts_com_snowplowanalytics_snowplow_web_page_1" -> SnowflakeDatatype.JsonArray
    val result = Loader.getShredType(columnName)

    result must beRight(expected)
  }

  def e2 = {
    val columnName = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"
    val expected = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" -> SnowflakeDatatype.JsonObject
    val result = Loader.getShredType(columnName)

    result must beRight(expected)
  }

  def e3 = {
    // starts with context_ instead of contexts_
    val columnName = "context_com_snowplowanalytics_snowplow_web_page_1"
    val result = Loader.getShredType(columnName)

    result must beLeft
  }

  def e4 = {
    val newColumns = List("contexts_com_acme_ctx_1", "unknown_type")
    Loader.getColumns(newColumns) must throwA[RuntimeException]
  }

  def e5 = {
    val config = Config(
      auth = Config.CredentialsAuth(
        accessKeyId = "accessKey",
        secretAccessKey = "secretKey"
      ),
      awsRegion = "awsRegion",
      manifest = "snoflake-manifest",
      snowflakeRegion = "ue-east-1",
      stage = "snowplow-stage",
      stageUrl = Config.S3Folder.coerce("s3://somestage/foo"),
      username = "snowfplow-loader",
      password = Config.PlainText("super-secret"),
      input = Config.S3Folder.coerce("s3://snowflake/input/"),
      account = "snowplow-account",
      warehouse = "snowplow_wa",
      database = "database",
      schema = "not_an_atomic",
      maxError = None,
      jdbcHost = None)

    val runId = RunId.ProcessedRunId(
      "archive/enriched/run=2017-10-09-17-40-30/",
      addedAt = DateTime.now(),       // Doesn't matter
      processedAt = DateTime.now(),   // Doesn't matter
      List(
        "contexts_com_snowplowanalytics_snowplow_web_page_1",
        "contexts_com_snowplowanalytics_snowplow_web_page_2",
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"),
      s3("s3://acme-snowplow/snowflake/run=2017-10-09-17-40-30/"),
      "some-script",
      false)

    // Some to use .like matcher
    val result = Some(Loader.getInsertStatement(config, runId))

    result must beSome.like {
      case Insert.InsertQuery(schema, table, columns, Select(sColumns, sSchema, sTable)) =>
        // INSERT
        val schemaResult = schema must beEqualTo("not_an_atomic")
        val tableResult = table must beEqualTo("events")
        val columnsAmount = columns must haveLength(131)
        val exactColumns = columns must containAllOf(List(
          "unstruct_event_com_snowplowanalytics_snowplow_link_click_1",
          "contexts_com_snowplowanalytics_snowplow_web_page_2",
          "app_id",
          "page_url"))

        // SELECT
        val sSchemaResult = sSchema must beEqualTo("not_an_atomic")
        val sTableResult = sTable must beEqualTo("snowplow_tmp_run_2017_10_09_17_40_30")
        val sColumnsAmount = sColumns must haveLength(131)
        val sExactColumns = sColumns must containAllOf(List(
          Select.CastedColumn("enriched_data","unstruct_event_com_snowplowanalytics_snowplow_link_click_1", SnowflakeDatatype.JsonObject),
          Select.CastedColumn("enriched_data", "contexts_com_snowplowanalytics_snowplow_web_page_1", SnowflakeDatatype.JsonArray),
          Select.CastedColumn("enriched_data", "true_tstamp", SnowflakeDatatype.Timestamp),
          Select.CastedColumn("enriched_data", "refr_domain_userid", SnowflakeDatatype.Varchar(36))))

        schemaResult.and(tableResult).and(columnsAmount)
          .and(sSchemaResult).and(sTableResult).and(sColumnsAmount)
          .and(exactColumns).and(sExactColumns)
    }
  }

  def e6 = {
    val connection = new LoaderSpec.Mock()
    val config = Config(
      auth = Config.CredentialsAuth("access", "secret"),
      "us-east-1",
      "manifest",
      "eu-central-1",
      "archive-stage",
      Config.S3Folder.coerce("s3://archive/"),
      Config.S3Folder.coerce("s3://enriched-input/"),
      "user",
      Config.PlainText("pass"),
      "snowplow-acc",
      "wh",
      "db",
      "atomic",
      None,
      None)
    Loader.exec(LoaderSpec.Mock, connection, new loader.LoaderSpec.ProcessingManifestTest, config)
    val expected = List(
      "SHOW schemas LIKE 'atomic'",
      "SHOW stages LIKE 'archive-stage' IN atomic",
      "SHOW tables LIKE 'events' IN atomic",
      "SHOW file formats LIKE 'snowplow_enriched_json' IN atomic",
      "SHOW warehouses LIKE 'wh'",
      "USE WAREHOUSE wh",
      "ALTER WAREHOUSE wh RESUME",
      "New transaction snowplow_run_2017_12_10_14_30_35 started",
      "ALTER TABLE atomic.events ADD COLUMN contexts_com_acme_something_1 ARRAY",
      "CREATE TEMPORARY TABLE IF NOT EXISTS atomic.snowplow_tmp_run_2017_12_10_14_30_35 (enriched_data OBJECT NOT NULL)",
      // INSERT INTO
      "Transaction [snowplow_run_2017_12_10_14_30_35] successfully closed"
    )
    val result = connection.getResult
    result must containAllOf(expected).inOrder
  }
}

object LoaderSpec {

  class Mock {
    private val messages = collection.mutable.ListBuffer.newBuilder[String]

    private var transaction: Option[String] = None
    private var transactionNum = 0

    def log(message: String): Unit = {
      messages += message
    }

    def startTransaction(name: Option[String]): Unit =
      transaction match {
        case Some(current) =>
          log(s"Invalid state: new transaction started until current [$current] not commited")
        case None =>
          log(s"New transaction ${name.getOrElse(" ")} started")
          transactionNum += 1
          val transactionName = name.getOrElse(transactionNum.toString)
          transaction = Some(name.getOrElse(transactionName))
      }

    def commitTransaction(): Unit =
      transaction match {
        case Some(current) =>
          log(s"Transaction [$current] successfully closed")
        case None =>
          log("Invalid state: trying to close non-existent transaction")
      }

    def rollbackTransaction(): Unit =
      transaction match {
        case Some(current) =>
          log(s"Transaction [$current] cancelled")
        case None =>
          log("Invalid state: trying to rollback non-existent transaction")
      }

    def getResult: List[String] =
      messages.result().toList
  }

  object Mock extends Connection[Mock] {
    def getConnection(config: Config): Mock = {
      val logConnection = new Mock
      logConnection.log(s"Connected to ${config.database} database")
      logConnection
    }

    def execute[S: Statement](connection: Mock, ast: S): Unit =
      connection.log(ast.getStatement.value)

    def startTransaction(connection: Mock, name: Option[String]): Unit =
      connection.startTransaction(name)

    def commitTransaction(connection: Mock): Unit =
      connection.commitTransaction()

    def executeAndOutput[S: Statement](connection: Mock, ast: S): Unit =
      connection.log(ast.getStatement.value)

    def rollbackTransaction(connection: Mock): Unit =
      connection.rollbackTransaction()

    def executeAndCountRows[S: Statement](connection: Mock, ast: S): Int = {
      connection.log(ast.getStatement.value)
      1 // Used for preliminary checks
    }
  }

  class ProcessingManifestTest extends ProcessManifest.Loader {
    private val loaded = collection.mutable.ListBuffer.newBuilder[(String, String)]

    def markLoaded(tableName: String, runid: String): Unit = {
      loaded += ((tableName, runid))
    }

    override def scan(tableName: String): Either[String, List[RunId]] = Right(
      List(
        RunId.ProcessedRunId(
          "enriched/good/run=2017-12-10-14-30-35",
          DateTime.parse("2017-12-10T01:20+02:00"),
          DateTime.parse("2017-12-10T01:20+02:00"),
          List("contexts_com_acme_something_1"),
          Config.S3Folder.coerce("s3://archive/run=2017-12-10-14-30-35/"), "0.2.0", false))
    )
  }
}
