/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowflake.core

import cats.implicits._
import java.util.{Map => JMap}

import scala.annotation.tailrec
import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._
import scala.util.control.NonFatal
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.joda.time.{DateTime, DateTimeZone}
import com.snowplowanalytics.snowplow.analytics.scalasdk.RunManifests
import com.snowplowanalytics.snowflake.core.Config.S3Folder
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/**
  * Entity responsible for getting all information about (un)processed folders
  */
trait ProcessManifest extends Product with Serializable {
  // Loader-specific functions
  def markLoaded(tableName: String, runId: String): Unit
  def scan(tableName: String): Either[String, List[RunId]]

  // Transformer-specific functions
  def add(tableName: String, runId: String): Unit
  def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): Unit
  def getUnprocessed(manifestTable: String, enrichedInput: S3Folder): Either[String, List[String]]
}

/**
 * Helper module for working with process manifest
 */
object ProcessManifest {

  type DbItem = JMap[String, AttributeValue]

  /** Singleton client to be recreated on failure (#51) */
  var dynamodbClient: AmazonDynamoDB = _

  /** Parameters for client recreation */
  val maxRetries = 5
  var region: String = ""

  /** Get DynamoDB client */
  def buildDynamoDb(awsRegion: String) = {
    val credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials
    val provider = new AWSStaticCredentialsProvider(credentials)
    dynamodbClient = AmazonDynamoDBClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
    region = awsRegion
  }

  /** Run a DynamoDB query; recreate the client on expired token exception */
  def runDynamoDbQuery[T](query: => T): T = {
    for (_ <- 1 until maxRetries) {
      try {
        val result = query
        result
      } catch {
        case e: AmazonServiceException if e.getMessage.contains("The security token included in the request is expired") => buildDynamoDb(region)
      }
    }

    /** Do not attempt to recreate the client after maxRetries attempts */
    val result = query
    result
  }

  /** Get S3 client */
  def getS3(awsRegion: String) = {
    val credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials
    val provider = new AWSStaticCredentialsProvider(credentials)
    AmazonS3ClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
  }

  trait Loader {
    def markLoaded(tableName: String, runId: String): Unit
    def scan(tableName: String): Either[String, List[RunId]]
  }

  /** Entity being able to return processed folders from real-world DynamoDB table */
  trait AwsScan {

    /** Get all folders with their state */
    def scan(tableName: String): Either[String, List[RunId]] = {

      def getRequest = new ScanRequest().withTableName(tableName)

      @tailrec def go(last: ScanResult, acc: List[DbItem]): List[DbItem] = {
        Option(last.getLastEvaluatedKey) match {
          case Some(key) =>
            val req = getRequest.withExclusiveStartKey(key)
            val response = runDynamoDbQuery[ScanResult] { ProcessManifest.dynamodbClient.scan(req) }
            val items = response.getItems
            go(response, items.asScala.toList ++ acc)
          case None => acc
        }
      }

      val scanResult = try {
        val firstResponse = runDynamoDbQuery[ScanResult] { ProcessManifest.dynamodbClient.scan(getRequest) }
        val initAcc = firstResponse.getItems.asScala.toList
        Right(go(firstResponse, initAcc).map(_.asScala))
      } catch {
        case NonFatal(e) => Left(e.toString)
      }

      for {
        items <- scanResult
        result <- items.map(RunId.parse).sequence
      } yield result
    }
  }

  /** Entity being able to mark folder as processed in real-world DynamoDB table */
  trait AwsLoader { Loader =>

    def markLoaded(tableName: String, runId: String): Unit = {
      val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt

      val request = new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(Map(
          RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
        ).asJava)
        .withAttributeUpdates(Map(
          "LoadedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
          "LoadedBy" -> new AttributeValueUpdate().withValue(new AttributeValue(ProjectMetadata.version))
        ).asJava)

      val _ = runDynamoDbQuery[UpdateItemResult] { ProcessManifest.dynamodbClient.updateItem(request) }
    }
  }

  case class AwsProcessingManifest(s3Client: AmazonS3)
    extends ProcessManifest
      with Loader
      with AwsLoader
      with AwsScan {

    def add(tableName: String, runId: String): Unit = {
      val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt

      val request = new PutItemRequest()
        .withTableName(tableName)
        .withItem(Map(
          RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId),
          "AddedAt" -> new AttributeValue().withN(now.toString),
          "AddedBy" -> new AttributeValue(ProjectMetadata.version),
          "ToSkip" -> new AttributeValue().withBOOL(false)
        ).asJava)

      val _ = runDynamoDbQuery[PutItemResult] { ProcessManifest.dynamodbClient.putItem(request) }
    }

    def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): Unit = {
      val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt
      val shredTypesDynamo = shredTypes.map(t => new AttributeValue(t)).asJava

      val request = new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(Map(
          RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
        ).asJava)
        .withAttributeUpdates(Map(
          "ProcessedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
          "ShredTypes" -> new AttributeValueUpdate().withValue(new AttributeValue().withL(shredTypesDynamo)),
          "SavedTo" -> new AttributeValueUpdate().withValue(new AttributeValue(Config.fixPrefix(outputPath)))
        ).asJava)

      val _ = runDynamoDbQuery[UpdateItemResult] { ProcessManifest.dynamodbClient.updateItem(request) }
    }

    /** Check if set of run ids contains particular folder */
    def contains(state: List[RunId], folder: String): Boolean =
      state.map(folder => folder.runId).contains(folder)

    def getUnprocessed(manifestTable: String, enrichedInput: S3Folder): Either[String, List[String]] = {
      val allRuns = RunManifests.listRunIds(s3Client, enrichedInput.path)

      scan(manifestTable) match {
        case Right(state) => Right(allRuns.filterNot(run => contains(state, run)))
        case Left(error) => Left(error)
      }
    }
  }

  case object DryRunProcessingManifest extends Loader with AwsScan {
    def markLoaded(tableName: String, runId: String): Unit =
      println(s"Marking runid [$runId] processed (dry run)")
  }

  case object AwsLoaderProcessingManifest extends Loader with AwsLoader with AwsScan
}
