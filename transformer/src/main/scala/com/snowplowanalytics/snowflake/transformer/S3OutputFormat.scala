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
package com.snowplowanalytics.snowflake.transformer

import com.netflix.bdp.s3.S3DirectoryOutputCommitter
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{OutputCommitter, RecordWriter, TaskAttemptContext}

/**
  * Implementation of `OutputFormat` existing just to embed Netflix `s3committer`
  * It cannot be added though CLI configs
  */
class S3OutputFormat[K, V] extends org.apache.hadoop.mapreduce.lib.output.FileOutputFormat[K, V] {
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[K, V] =
    throw new NotImplementedError("S3OutputFormat defined only for getOutputCommitter")

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    println("S3OutputFormat getOutputCommitter")
    new S3DirectoryOutputCommitter(FileOutputFormat.getOutputPath(context), context)
  }
}
