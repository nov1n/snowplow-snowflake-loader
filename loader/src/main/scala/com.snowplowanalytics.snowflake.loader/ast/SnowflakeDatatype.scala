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
package com.snowplowanalytics.snowflake.loader.ast

sealed trait SnowflakeDatatype

object SnowflakeDatatype {
  case class Varchar(size: Option[Int]) extends SnowflakeDatatype
  case object Timestamp extends SnowflakeDatatype
  case class Char(size: Option[Int]) extends SnowflakeDatatype
  case object SmallInt extends SnowflakeDatatype
  case object DoublePrecision extends SnowflakeDatatype
  case object Integer extends SnowflakeDatatype
  case class Number(precision: Int, scale: Int) extends SnowflakeDatatype
  case object Boolean extends SnowflakeDatatype
  case object Variant extends SnowflakeDatatype
  case object JsonObject extends SnowflakeDatatype
  case object JsonArray extends SnowflakeDatatype
}
