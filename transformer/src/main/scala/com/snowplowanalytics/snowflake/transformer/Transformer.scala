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

import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.{Data, EventTransformer}
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig

object Transformer {

  implicit val formats = DefaultFormats

  /**
    * Transform TSV to pair of shredded keys and enriched event in JSON format
    * @param line enriched event TSV
    * @param eventsManifestConfig events manifest config instance
    * @return pair of set with column names and JSON string, ready to be saved
    */
  def transform(line: String, eventsManifestConfig: Option[EventsManifestConfig]): Option[(Set[String], String)] = {
    EventTransformer.jsonifyGoodEvent(line.split("\t", -1)) match {
      case Right((inventory, json)) =>
        val shredTypes = inventory.map(item => Data.fixSchema(item.shredProperty, item.igluUri))
        EventsManifestSingleton.get(eventsManifestConfig) match {
          case Some(manifest) =>
            val etlTstamp = (json \ "etl_tstamp").extract[String].replace("T", " ").replace("Z","")
            if (manifest.put((json \ "event_id").extract[String], (json \ "event_fingerprint").extract[String], etlTstamp)) {
              Some(shredTypes, compact(json))
            } else None
          case None => Some(shredTypes, compact(json))
        }
      case Left(e) =>
        throw new RuntimeException(e.mkString("\n"))
    }
  }
}
