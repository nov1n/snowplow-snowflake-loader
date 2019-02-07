/*
 * Copyright (c) 2017-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowflake.core.Config._

case class TransformerJobConfig(enrichedArchive: S3Folder, snowflakeOutput: S3Folder, errorEventsOutput: Option[S3Folder], runId: String) {
  def input: String = {
    val (enrichedBucket, enrichedPath) = enrichedArchive.splitS3Folder
    s"s3a://$enrichedBucket/$enrichedPath$runIdFolder/*"
  }

  def output: String = {
    val (bucket, path) = snowflakeOutput.splitS3Folder
    s"s3a://$bucket/$path$runIdFolder"
  }

  def errorOutput: Option[String] = {
    errorEventsOutput match {
      case Some(f) =>
        val (bucket, path) = f.splitS3Folder
        Some(s"s3a://$bucket/$path$runIdFolder")
      case None => None
    }
  }

  def runIdFolder: String = runId.split("/").last
}

