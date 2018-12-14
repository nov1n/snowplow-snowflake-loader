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
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest

/** Singletons needed for unserializable or stateful classes. */
object singleton {

  /** Singleton for EventsManifest to maintain one per node. */
  object EventsManifestSingleton {
    import EventsManifest._

    @volatile private var instance: Option[EventsManifest] = _

    /**
      * Retrieve or build an instance of EventsManifest.
      * @param eventsManifestConfig configuration for EventsManifest
      */
    def get(eventsManifestConfig: Option[EventsManifestConfig]): Option[EventsManifest] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = eventsManifestConfig.map(initStorage) match {
              case Some(v) => v.fold(exception => throw new RuntimeException(exception.toString), manifest => Some(manifest))
              case None => None
            }
          }
        }
      }
      instance
    }
  }
}