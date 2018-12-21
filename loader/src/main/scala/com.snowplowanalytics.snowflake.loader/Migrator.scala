/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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

import ast._
import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.loader.ast.AlterTable.AlterColumnDatatype
import connection.Jdbc

/** Module containing functions to migrate Snowflake tables */
object Migrator {

  /** Run migration process */
  def run(config: Config, loaderVersion: String): Unit = {
    loaderVersion match {
      case "0.4.0" =>
        val connection = Jdbc.getConnection(config)

        // Save only static credentials
        val credentials = PasswordService.getSetupCredentials(config.auth)

        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "user_ipaddress", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "user_fingerprint", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "domain_userid", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "network_userid", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "geo_region", SnowflakeDatatype.Char(3)))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "ip_organization", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "ip_domain", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "refr_domain_userid", SnowflakeDatatype.Varchar(Some(128))))
        Jdbc.executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, "domain_sessionid", SnowflakeDatatype.Char(128)))

        connection.close()
      case _ =>
        val message = s"Unrecognized Snowplow Snowflake Loader version: $loaderVersion. (Supported: 0.4.0)"
        System.err.println(message)
        sys.exit(1)
    }
  }
}
