/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import java.nio.ByteBuffer
import scala.util.Random
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext.DoubleAccumulatorParam
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes


/**
 * Advanced Kinesis Spark Streaming WordCount example.
 *
 * See http://spark.apache.org/docs/latest/streaming-kinesis.html for more details on
 *   the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receiver) per shard 
 *   for the given stream.
 * It then starts pulling from the last checkpointed sequence number of the given 
 *   <stream-name> and <endpoint-url>. 
 *
 * Valid endpoint urls:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
 * 
 * This code uses the DefaultAWSCredentialsProviderChain and searches for credentials
 *   in the following order of precedence:
 * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 * Java System Properties - aws.accessKeyId and aws.secretKey
 * Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 * Instance profile credentials - delivered through the Amazon EC2 metadata service
 *
 * Usage: AdvancedKinesisWordCountASL <stream-name> <endpoint-url>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/bin/run-example \
 *        org.apache.spark.examples.streaming.AdvancedKinesisWordCountASL mySparkStream \
 *        https://kinesis.us-east-1.amazonaws.com
 */
object AdvancedKinesisWordCountASL extends Logging {
  def main(args: Array[String]) {
    /* Check that all required args were passed in. */
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: KinesisWordCount <stream-name> <endpoint-url>
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    /* Populate the appropriate variables from the given args */
    val Array(streamName, endpointUrl) = args

    /* Determine the number of shards from the stream */
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards()
      .size()

    /* In this example, we're going to create 1 Kinesis Worker/Receiver/DStream for each shard. */
    val numStreams = numShards

    /* 
     *  numSparkThreads should be 1 more thread than the number of receivers.
     *  This leaves one thread available for actually processing the data.
     */
    val numSparkThreads = numStreams + 1

    /* Setup the and SparkConfig and StreamingContext */
    /* Spark Streaming batch interval */
    val batchInterval = Milliseconds(2000)
    val checkpointPath = "/tmp/checkpoint"

    /* Function to create and setup a new StreamingContext */
    def createStreamingContext(): StreamingContext = {
      val sparkConfig = new SparkConf().setAppName("AdvancedKinesisWordCount")
      .setMaster(s"local[$numSparkThreads]")
      //.set("spark.cleaner.ttl", "60")
      // Let Spark Streaming figure out when to unpersist RDDs 
      .set("spark.streaming.unpersist", "true")
      // Setup FAIR scheduling
      .set("spark.scheduler.allocation.file", "/Users/cfregly/fairscheduler.xml")
      .set("spark.scheduler.pool", "kinesisPool")
      .set("spark.scheduler.mode", "FAIR")
      
      val ssc = new StreamingContext(sparkConfig, batchInterval)
      ssc.checkpoint(checkpointPath)
      ssc
    }

    /* Rebuild Streaming from a checkpoint file if one exists, otherwise create a new one. */
    //val ssc = StreamingContext.getOrCreate(checkpointPath, createStreamingContext)
    val ssc = createStreamingContext()

    /* Kinesis checkpoint interval.  Same as batchInterval for this example. */
    val kinesisCheckpointInterval = batchInterval

    /* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards  */
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, streamName, endpointUrl, kinesisCheckpointInterval,
        InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
    }

    // Union all the streams 
    val unionStreams = ssc.union(kinesisStreams)

    // Convert each line of Array[Byte] to String, split into words, and count them 
    val wordsDStream = unionStreams.flatMap(byteArray => new String(byteArray)
      .split(" ").map(word => word.toInt)).cache()

    var globalBatchCount = 0L
    var globalRDDCount = 0L
    var globalSum = 0L

    // Increment the globalBatchCount (number of batches)
    wordsDStream.foreachRDD(batchRDD => {
    globalBatchCount += 1
      // Increment the globalSum (sum of words in all RDDs)
      globalSum += batchRDD.fold(0)(_+_)
    })

    // Increment the globalRDDCount (number of RDDs in all batches)
    wordsDStream.count().foreachRDD(countRDD => {globalRDDCount += countRDD.first()})

    // Map each word to a (word, 1) tuple so we can reduce/aggregate by key. 
    val wordCountsMappedDStream = wordsDStream.map(word => (word, 1))

    /* 
     * Reduce/aggregate by key.
     * Note:  These methods are picked up by the PairDStreamFunctions implicit
     */
    val wordCountsDStream = wordCountsMappedDStream.reduceByKey(_ + _).cache()

    /*
     *  Calculate word counts for the given window.
     *  Use the inverse function trick to incrementally update the window counts versus
     *    completely recalculating for each window.
     */
    val wordCountsByWindowDStream = wordCountsMappedDStream
      .reduceByKeyAndWindow(_ + _, _ - _, batchInterval * 2, batchInterval)

    // Sort the underylying RDD batches
    val wordCountsByWindowSortedDStream = wordCountsByWindowDStream.transform(
      windowRDDBatch => windowRDDBatch.sortByKey(true))

    // Materialize and print the counts by window 
    wordCountsByWindowSortedDStream.foreach(rdd => {
      rdd.foreach(row => println((row._1, row._2,  globalBatchCount, globalRDDCount, globalSum)))
    })

    /**
     * Update the running totals of words.
     *
     * @param sequence of new counts
     * @param current running total (could be None if no current count exists)
     */
    def updateTotals = (newCounts: Seq[Int], currentCounts: Option[Int]) => {
      val newCount = newCounts.foldLeft(0)((left, right) => left + right)
      val currentCount = currentCounts.getOrElse(0)
      Some(newCount + currentCount)
    }

    /*
     *  Calculate the running totals using the updateTotals method.
     *	Note:  State and Window operations are implicitly cache()'d
     */
    val wordCountTotalsDStream = wordCountsDStream.updateStateByKey[Int](updateTotals)

    // Sort the totals by transforming each underlying RDD batch with sortByKey()
    val wordCountTotalsSortedDStream = wordCountTotalsDStream.transform(totalRDDBatch => {
      totalRDDBatch.sortByKey(true)
    }).cache()

    /* 
     * Use DStream checkpointing for trimming the infinitely-growing lineage
     * 	 of stateful and window-based DStreams.  Writes latest DStream/RDD data to disk.
     */
    val dstreamCheckpointInterval = batchInterval
    wordCountTotalsSortedDStream.checkpoint(dstreamCheckpointInterval)

    // Materialize and print the totals
    wordCountTotalsSortedDStream.foreach(rddBatch => {
      rddBatch.foreach(row => println((row._1, row._2, globalBatchCount, globalRDDCount, globalSum)))
    })

    // Start the streaming context and await termination 
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * Usage: AdvancedKinesisWordCountProducerASL <stream-name> <kinesis-endpoint-url>
 *     <recordsPerSec> <wordsPerRecord>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <kinesis-endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <records-per-sec> is the rate of records per second to put onto the stream
 *   <words-per-record> is the rate of records per second to put onto the stream
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/bin/run-example \
 *         org.apache.spark.examples.streaming.AdvancedKinesisWordCountProducerASL mySparkStream \
 *         https://kinesis.us-east-1.amazonaws.com 10 5
 */
object AdvancedKinesisWordCountProducerASL {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: AdvancedKinesisWordCountProducerASL <stream-name> <endpoint-url>" +
          " <records-per-sec> <words-per-record>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    /* Populate the appropriate variables from the given args */
    val Array(stream, endpoint, recordsPerSecond, wordsPerRecord) = args

    /* Generate the records and return the totals */
    val totals = generate(stream, endpoint, recordsPerSecond.toInt, wordsPerRecord.toInt)

    /* Print the array of (index, total) tuples */
    println("Total Counts of Records Produced")
    totals.foreach(total => println(total.toString()))
  }

  def generate(stream: String,
      endpoint: String,
      recordsPerSecond: Int,
      wordsPerRecord: Int): Seq[(Int, Int)] = {

    val MaxRandomInts = 10

    /* Create the Kinesis client */
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpoint)

    println(s"Putting records onto stream $stream and endpoint $endpoint at a rate of" +
      s" $recordsPerSecond records per second and $wordsPerRecord words per record");

    val totals = new Array[Int](MaxRandomInts)
    /* Put String records onto the stream per the given recordPerSec and wordsPerRecord */
    for (i <- 1 to 1) {
      /* Generate recordsPerSec records to put onto the stream */
      val records = (1 to recordsPerSecond.toInt).map { recordNum =>
        /* 
         *  Randomly generate each wordsPerRec words between 0 (inclusive)
         *  and MAX_RANDOM_INTS (exclusive) 
         */
        val data = (1 to wordsPerRecord.toInt).map(x => {
          /* Generate the random int */
          val randomInt = Random.nextInt(MaxRandomInts)

          /* Keep track of the totals */
          totals(randomInt) += 1

          randomInt.toString()
        }).mkString(" ")

        /* Create a partitionKey based on recordNum */
        val partitionKey = s"partitionKey-$recordNum"

        /* Create a PutRecordRequest with an Array[Byte] version of the data */
        val putRecordRequest = new PutRecordRequest().withStreamName(stream)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()));

        /* Put the record onto the stream and capture the PutRecordResult */
        val putRecordResult = kinesisClient.putRecord(putRecordRequest);
        println(s"Sent: $data") 
      }

      /* Sleep for a second */
      Thread.sleep(1000)
      //println("Sent " + recordsPerSecond + " records")
    }

    /* Convert the totals to (index, total) tuple */
    (0 to (MaxRandomInts - 1)).zip(totals)
  }
}
