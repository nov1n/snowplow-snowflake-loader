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

// cats
import cats.data.Validated.{Invalid, Valid}

// scala-analytics-sdk
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent

// events-manifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig

// This library
import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton

object Transformer {

  /**
    * Transform jsonified TSV to pair of shredded keys and enriched event in JSON format
    *
    * @param event                Event case class instance
    * @param eventsManifestConfig events manifest config instance
    * @return pair of set with column names and JValue
    */
  def transform(event: Event, eventsManifestConfig: Option[EventsManifestConfig]): Option[(Set[String], String)] = {
    val shredTypes = event.inventory.map(item => SnowplowEvent.transformSchema(item.shredProperty, item.schemaKey))
    val eventId = event.event_id.toString
    val eventFingerprint = event.event_fingerprint.getOrElse("")
    val etlTstamp = event.etl_tstamp.map(i => EventsManifest.RedshiftTstampFormatter.format(i)).getOrElse("")

    EventsManifestSingleton.get(eventsManifestConfig) match {
      case Some(manifest) =>
        if (manifest.put(eventId, eventFingerprint, etlTstamp)) {
          Some((shredTypes, event.toJson(true).noSpaces))
        } else None
      case None => Some((shredTypes, event.toJson(true).noSpaces))
    }
  }

  /**
    * Transform TSV to pair of inventory items and JSON object
    *
    * @param line enriched event TSV
    * @return Event case class instance
    */
  def jsonify(line: String): Event = {
    Event.parse(line) match {
      case Valid(event) => event
      case Invalid(e) =>
        throw new RuntimeException(e.toList.mkString("\n"))
    }
  }
}
