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

import com.snowplowanalytics.snowflake.loader.ast.CreateTable._
import com.snowplowanalytics.snowflake.loader.ast.SnowflakeDatatype._

object AtomicDef {
  /**
   * List of columns for enriched event
   */
  val columns = List(
    // App
    Column("app_id", Varchar(255)),
    Column("platform", Varchar(255)),

    // Data/time
    Column("etl_tstamp", Timestamp),
    Column("collector_tstamp", Timestamp, notNull = true),
    Column("dvce_created_tstamp", Timestamp),

    // Event
    Column("event", Varchar(128)),
    Column("event_id", Char(36), notNull = true, unique = true),
    Column("txn_id", Integer),

    // Namespacing and versioning
    Column("name_tracker", Varchar(128)),
    Column("v_tracker", Varchar(100)),
    Column("v_collector", Varchar(100), notNull = true),
    Column("v_etl", Varchar(100), notNull = true),

    // User id and visit
    Column("user_id", Varchar(255)),
    Column("user_ipaddress", Varchar(128)),
    Column("user_fingerprint", Varchar(128)),
    Column("domain_userid", Varchar(128)),
    Column("domain_sessionidx", SmallInt),
    Column("network_userid", Varchar(128)),

    // Location
    Column("geo_country", Char(2)),
    Column("geo_region", Char(2)),
    Column("geo_city", Varchar(75)),
    Column("geo_zipcode", Varchar(15)),
    Column("geo_latitude", DoublePrecision),
    Column("geo_longitude", DoublePrecision),
    Column("geo_region_name", Varchar(100)),

    // Ip lookups
    Column("ip_isp", Varchar(100)),
    Column("ip_organization", Varchar(128)),
    Column("ip_domain", Varchar(128)),
    Column("ip_netspeed", Varchar(100)),

    // Page
    Column("page_url", Varchar(4096)),
    Column("page_title", Varchar(2000)),
    Column("page_referrer", Varchar(4096)),

    // Page URL components
    Column("page_urlscheme", Varchar(16)),
    Column("page_urlhost", Varchar(255)),
    Column("page_urlport", Integer),
    Column("page_urlpath", Varchar(3000)),
    Column("page_urlquery", Varchar(6000)),
    Column("page_urlfragment", Varchar(3000)),

    // Referrer URL components
    Column("refr_urlscheme", Varchar(16)),
    Column("refr_urlhost", Varchar(255)),
    Column("refr_urlport", Integer),
    Column("refr_urlpath", Varchar(6000)),
    Column("refr_urlquery", Varchar(6000)),
    Column("refr_urlfragment", Varchar(3000)),

    // Referrer details
    Column("refr_medium", Varchar(25)),
    Column("refr_source", Varchar(50)),
    Column("refr_term", Varchar(255)),

    // Marketing
    Column("mkt_medium", Varchar(255)),
    Column("mkt_source", Varchar(255)),
    Column("mkt_term", Varchar(255)),
    Column("mkt_content", Varchar(500)),
    Column("mkt_campaign", Varchar(255)),

    // Custom structured event
    Column("se_category", Varchar(1000)),
    Column("se_action", Varchar(1000)),
    Column("se_label", Varchar(1000)),
    Column("se_property", Varchar(1000)),
    Column("se_value", DoublePrecision),

    // Ecommerce
    Column("tr_orderid", Varchar(255)),
    Column("tr_affiliation", Varchar(255)),
    Column("tr_total", Number(18,2)),
    Column("tr_tax", Number(18,2)),
    Column("tr_shipping", Number(18,2)),
    Column("tr_city", Varchar(255)),
    Column("tr_state", Varchar(255)),
    Column("tr_country", Varchar(255)),
    Column("ti_orderid", Varchar(255)),
    Column("ti_sku", Varchar(255)),
    Column("ti_name", Varchar(255)),
    Column("ti_category", Varchar(255)),
    Column("ti_price", Number(18,2)),
    Column("ti_quantity", Integer),

    // Page ping
    Column("pp_xoffset_min", Integer),
    Column("pp_xoffset_max", Integer),
    Column("pp_yoffset_min", Integer),
    Column("pp_yoffset_max", Integer),

    // Useragent
    Column("useragent", Varchar(1000)),

    // Browser
    Column("br_name", Varchar(50)),
    Column("br_family", Varchar(50)),
    Column("br_version", Varchar(50)),
    Column("br_type", Varchar(50)),
    Column("br_renderengine", Varchar(50)),
    Column("br_lang", Varchar(255)),
    Column("br_features_pdf", Boolean),
    Column("br_features_flash", Boolean),
    Column("br_features_java", Boolean),
    Column("br_features_director", Boolean),
    Column("br_features_quicktime", Boolean),
    Column("br_features_realplayer", Boolean),
    Column("br_features_windowsmedia", Boolean),
    Column("br_features_gears", Boolean),
    Column("br_features_silverlight", Boolean),
    Column("br_cookies", Boolean),
    Column("br_colordepth", Varchar(12)),
    Column("br_viewwidth", Integer),
    Column("br_viewheight", Integer),

    // Operating System
    Column("os_name", Varchar(50)),
    Column("os_family", Varchar(50)),
    Column("os_manufacturer", Varchar(50)),
    Column("os_timezone", Varchar(255)),

    // Device/Hardware
    Column("dvce_type", Varchar(50)),
    Column("dvce_ismobile", Boolean),
    Column("dvce_screenwidth", Integer),
    Column("dvce_screenheight", Integer),

    // Document
    Column("doc_charset", Varchar(128)),
    Column("doc_width", Integer),
    Column("doc_height", Integer),

    // Currency
    Column("tr_currency", Char(3)),
    Column("tr_total_base", Number(18,2)),
    Column("tr_tax_base", Number(18,2)),
    Column("tr_shipping_base", Number(18,2)),
    Column("ti_currency", Char(3)),
    Column("ti_price_base", Number(18,2)),
    Column("base_currency", Char(3)),

    // Geolocation
    Column("geo_timezone", Varchar(64)),

    // Click ID
    Column("mkt_clickid", Varchar(128)),
    Column("mkt_network", Varchar(64)),

    // ETL Tags
    Column("etl_tags", Varchar(500)),

    // Time event was sent
    Column("dvce_sent_tstamp", Timestamp),

    // Referer
    Column("refr_domain_userid", Varchar(128)),
    Column("refr_dvce_tstamp", Timestamp),

    // Session ID
    Column("domain_sessionid", Char(128)),

    // Derived timestamp
    Column("derived_tstamp", Timestamp),

    // Event schema
    Column("event_vendor", Varchar(1000)),
    Column("event_name", Varchar(1000)),
    Column("event_format", Varchar(128)),
    Column("event_version", Varchar(128)),

    // Event fingerprint
    Column("event_fingerprint", Varchar(128)),

    // True timestamp
    Column("true_tstamp", Timestamp)
  )

  /** Get statement to create standard table with custom schema */
  def getTable(schema: String = Defaults.Schema): CreateTable =
    CreateTable(schema, Defaults.Table, columns, Some(PrimaryKeyConstraint("event_id_pk", "event_id")))
}
