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
package org.apache.spark.streaming.kinesis

import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

/**
 * Helper class to create Amazon Kinesis Input Stream
 * :: Experimental ::
 */
@Experimental
object KinesisUtils {
  /**
   * Create an InputDStream that pulls messages from a Kinesis stream.
   * :: Experimental ::
   * @param appName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
   *                 by the Kinesis Client Library.  If you change the App name or Stream name,
   *                 the KCL will throw errors.  This usually requires deleting the backing  
   *                 DynamoDB table with the same name this Kinesis application.
   * @param ssc    StreamingContext object
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   * @param credentialsProvider  (optional) implementation of AWSCredentialsProvider 
   * 
   * @return ReceiverInputDStream[Array[Byte]]
   */
  @Experimental
  def createStream(
      appName:  String,
      ssc: StreamingContext,
      streamName: String,
      endpointUrl: String,
      checkpointInterval: Duration,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel,
      credentialsProvider: AWSCredentialsProvider
      ): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(new KinesisReceiver(appName, streamName, endpointUrl, checkpointInterval, 
      initialPositionInStream, storageLevel, credentialsProvider))
  }

  /**
   * Create a Java-friendly InputDStream that pulls messages from a Kinesis stream.
   * :: Experimental ::
   * @param appName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
   *                 by the Kinesis Client Library.  If you change the App name or Stream name,
   *                 the KCL will throw errors.  This usually requires deleting the backing  
   *                 DynamoDB table with the same name this Kinesis application.
   * @param jssc Java StreamingContext object
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   * @param credentialsProvider  (optional) implementation of AWSCredentialsProvider 
   * 
   * @return JavaReceiverInputDStream[Array[Byte]]
   */
  @Experimental
  def createStream(
	  appName:  String,
      jssc: JavaStreamingContext, 
      streamName: String, 
      endpointUrl: String, 
      checkpointInterval: Duration,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel,
      credentialsProvider: AWSCredentialsProvider
      ): JavaReceiverInputDStream[Array[Byte]] = {
    jssc.receiverStream(new KinesisReceiver(appName, streamName, endpointUrl, checkpointInterval, 
      initialPositionInStream, storageLevel, credentialsProvider))
  }
}
