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
package connection

import ast._
import java.sql.{DriverManager, SQLException, Connection => JdbcConnection}
import java.util.Properties

import scala.collection.mutable.ListBuffer

import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

object Jdbc extends Connection[JdbcConnection] {

  @throws[SQLException]
  def getConnection(config: Config): JdbcConnection = {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

    // US West is default: https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#jdbc-driver-connection-string
    val host = config.jdbcHost match {
      case Some(overrideHost) => overrideHost
      case None =>
        if (config.snowflakeRegion == "us-west-1")
          s"${config.account}.snowflakecomputing.com"
        else
          s"${config.account}.${config.snowflakeRegion}.snowflakecomputing.com"
    }
    // Build connection properties
    val properties = new Properties()

    val password = config.password match {
      case Config.PlainText(text) => text
      case Config.EncryptedKey(Config.EncryptedConfig(key)) =>
        PasswordService.getKey(key.parameterName) match {
          case Right(result) => result
          case Left(error) =>
            throw new RuntimeException(s"Cannot retrieve JDBC password from EC2 Parameter Store. $error")
        }
    }

    val userAgent = ProjectMetadata.name + "/" + ProjectMetadata.version

    properties.put("user", config.username)
    properties.put("password", password)
    properties.put("account", config.account)
    properties.put("warehouse", config.warehouse)
    properties.put("db", config.database)
    properties.put("schema", config.schema)
    properties.put("userAgent", userAgent)

    val connectStr = s"jdbc:snowflake://$host"
    DriverManager.getConnection(connectStr, properties)
  }

  /** Execute SQL statement */
  def execute[S: Statement](connection: JdbcConnection, ast: S): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute(ast.getStatement.value)
    jdbcStatement.close()
  }

  /** Begin transaction */
  def startTransaction(connection: JdbcConnection, name: Option[String]): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute(s"BEGIN TRANSACTION NAME ${name.getOrElse("")}")
    jdbcStatement.close()
  }

  /** Commit transaction */
  def commitTransaction(connection: JdbcConnection): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute("COMMIT")
    jdbcStatement.close()
  }

  def rollbackTransaction(connection: JdbcConnection): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute("ROLLBACK")
    jdbcStatement.close()
  }

  /** Execute SQL statement and print status */
  def executeAndOutput[S: Statement](connection: JdbcConnection, ast: S): Unit = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  /** Execute SQL query and count rows */
  def executeAndCountRows[S: Statement](connection: JdbcConnection, ast: S): Int = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    var i = 0
    while (rs.next()) {
      i = i + 1
    }
    i
  }

  /** Execute SQL query and return result */
  def executeAndReturnResult[S: Statement](connection: JdbcConnection, ast: S): List[Map[String, Object]] = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    val metadata = rs.getMetaData
    var result = new ListBuffer[Map[String, Object]]()

    while (rs.next()) {
      var row = (for (i <- 1 to metadata.getColumnCount) yield metadata.getColumnName(i) -> rs.getObject(i)).toMap
      result += row
    }

    rs.close()
    statement.close()
    result.toList
  }
}
