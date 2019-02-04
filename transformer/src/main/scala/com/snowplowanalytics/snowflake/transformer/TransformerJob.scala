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

import org.apache.spark.sql.{SaveMode, SparkSession}

import com.snowplowanalytics.snowflake.core.ProcessManifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig

object TransformerJob {

  /** Process all directories, saving state into DynamoDB */
  def run(spark: SparkSession, manifest: ProcessManifest, tableName: String, jobConfigs: List[TransformerJobConfig], eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean): Unit = {
    jobConfigs.foreach { jobConfig =>
      println(s"Snowflake Transformer: processing ${jobConfig.runId}. ${System.currentTimeMillis()}")
      manifest.add(tableName, jobConfig.runId)
      val shredTypes = process(spark, jobConfig, eventsManifestConfig, inbatch)
      manifest.markProcessed(tableName, jobConfig.runId, shredTypes, jobConfig.output)
      println(s"Snowflake Transformer: processed ${jobConfig.runId}. ${System.currentTimeMillis()}")
    }
  }

  /**
    * Transform particular folder to Snowflake-compatible format and
    * return list of discovered shredded types
    *
    * @param spark                Spark SQL session
    * @param jobConfig            configuration with paths
    * @param eventsManifestConfig events manifest config instance
    * @param inbatch              whether inbatch deduplication should be used
    * @return list of discovered shredded types
    */
  def process(spark: SparkSession, jobConfig: TransformerJobConfig, eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean) = {
    import spark.implicits._

    val sc = spark.sparkContext
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val events = sc
      .textFile(jobConfig.input)
      .map { e => Transformer.jsonify(e) }
    val dedupedEvents = if (inbatch) {
      events
        .groupBy { e => (e.event_id, e.event_fingerprint) }
        .flatMap { case (_, vs) => vs.take(1) }
    } else events
    val snowflake = dedupedEvents.flatMap { e =>
      Transformer.transform(e, eventsManifestConfig) match {
        case Some((keys, transformed)) =>
          keysAggregator.add(keys)
          Some(transformed)
        case None => None
      }
    }

    // DataFrame is used only for S3OutputFormat
    snowflake.toDF.write.mode(SaveMode.Append).text(jobConfig.output)

    val keysFinal = keysAggregator.value.toList
    println(s"Shred types for  ${jobConfig.runId}: " + keysFinal.mkString(", "))
    keysAggregator.reset()
    keysFinal
  }
}
