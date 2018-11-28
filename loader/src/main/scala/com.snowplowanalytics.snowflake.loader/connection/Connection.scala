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
import com.snowplowanalytics.snowflake.core.Config

// TODO: rewrite as type-class
/** DB-connection adapter */
trait Connection[C] {
  def getConnection(config: Config): C
  def execute[S: Statement](connection: C, ast: S): Unit
  def startTransaction(connection: C, name: Option[String]): Unit
  def commitTransaction(connection: C): Unit
  def rollbackTransaction(connection: C): Unit
  def executeAndOutput[S: Statement](connection: C, ast: S): Unit
  def executeAndCountRows[S: Statement](connection: C, ast: S): Int
  def executeAndReturnResult[S: Statement](connection: C, ast: S): List[Map[String, Object]]
}
