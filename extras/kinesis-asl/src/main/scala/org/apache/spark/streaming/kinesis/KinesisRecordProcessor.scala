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

import java.util.List
import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Random
import org.apache.spark.Logging
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.streaming.receiver.BlockGenerator
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.BlockGeneratorListener
import org.apache.spark.SparkEnv
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis Worker creates an instance of this KinesisRecordProcessor for each 
 *   shard in the Kinesis stream upon startup.  This is normally done in separate threads, 
 *   but the KCLs within the KinesisReceivers will balance themselves out if you create 
 *   multiple Receivers.
 *
 * @param receiver Kinesis receiver
 * @param workerId to track sequence numbers and 
 */
private[kinesis] class KinesisRecordProcessor(
    receiver: KinesisReceiver,
    workerId: String) extends IRecordProcessor with Logging {

  // shardId to be populated during initialize()
  private var shardId: String = _

  // TODO:  figure this out for storing the latest checkpointer or sequence number within a block
  //        before it's generated
  // Latest sequence number within a not-yet-generated block 
  //private var latestSeqNumberWithinNotYetGeneratedBlock: String = _
  private var latestCheckpointerWithinNotYetGeneratedBlock: IRecordProcessorCheckpointer = _

  /*
   * Last sequence number of a generated block
   * StreamBlockId :: seqNumber (String)
   */
  //TODO:  figure this out
  //private var lastSeqNumberOfGeneratedBlockMap: ConcurrentHashMap[StreamBlockId, String] = _  
  private var lastCheckpointerOfGeneratedBlockMap: 
    ConcurrentHashMap[StreamBlockId, IRecordProcessorCheckpointer] = _
  

  /*
   * Manage the BlockGenerator in the RecordProcessor itself to better manage block store offset
   * commits back to the Kinesis source (ie. Checkpointing or ACK'ing).
   */
  private var blockGenerator: BlockGenerator = _
 
  /**
   * The Kinesis Client Library calls this method during IRecordProcessor initialization.
   *
   * @param shardId assigned by the KCL to this particular RecordProcessor.
   */
  override def initialize(shardId: String) {
    this.shardId = shardId

    lastCheckpointerOfGeneratedBlockMap = 
      new ConcurrentHashMap[StreamBlockId, IRecordProcessorCheckpointer]()

    // Create a new BlockGenerator by implementing the BlockGeneratorListener
    blockGenerator = new BlockGenerator(new BlockGeneratorListener() {
    /*
     * BlockGenerator callbacks
     */
    /**
     *  Update latest sequence number within a block that has not yet been generated.
     *  This is called when an element is added to a not-yet-generated block.
     */
    def onAddData(data: Any, metadata: Any): Unit = {
      // TODO:  we dont have the sequence number, unfortunately!  we need to pass Checkpointer
      //        in metadata?!
      // Update the sequence number of the data that was added to the generator
      if (metadata != null) {
        //latestSeqNumberWithinNotYetGeneratedBlock = metadata.asInstanceOf[String]
        latestCheckpointerWithinNotYetGeneratedBlock = 
          metadata.asInstanceOf[IRecordProcessorCheckpointer]
      }

      // TODO:  Should we log or error out if metadata == null?
    }

    /**
     * Update the latest sequence number for the generated block.
     * This is called when a block is generated but not yet stored.
     */
     def onGenerateBlock(blockId: StreamBlockId): Unit = {
       // Remember the offsets of shards when a block has been generated
       //TODO:  Figure this out
       //lastSeqNumberOfGeneratedBlockMap.put(blockId, latestCheckpointerWithinNotYetGeneratedBlock)
       lastCheckpointerOfGeneratedBlockMap.put(blockId, 
         latestCheckpointerWithinNotYetGeneratedBlock)
       // reset latest sequence number back to null
       //TODO: Figure this out
       //latestSeqNumberWithinNotYetGeneratedBlock = null
       latestCheckpointerWithinNotYetGeneratedBlock = null
     }

     /**
      * Store the ready-to-be-stored block and checkpoint the sequence number back to the source.
      * This is called when a block is ready to be stored (pushed).
      */
      def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
        // Store block and checkpoint the sequence number back to the source
        storeBlockAndCheckpointSequenceNumber(blockId, arrayBuffer)
      }

      def onError(message: String, throwable: Throwable): Unit = {
        //TODO:  figure out what this does and/or log an error
        receiver.reportError(message, throwable)
        //logger.
      }
    }, receiver.streamId, SparkEnv.get.conf)

    // Start the BlockGenerator
    blockGenerator.start()

    logInfo(s"Initialized workerId $workerId with shardId $shardId and started BlockGenerator")
  }

  /**
   * Store the ready-to-be-stored block (multiple  and checkpoint the sequence number back to the source.
   * Stop the receiver if store() retries are exceeded.
   */
  private def storeBlockAndCheckpointSequenceNumber(blockId: StreamBlockId, 
      arrayBuffer: ArrayBuffer[_]): Unit = {

    //TODO:  convert this to retryRandomOffset()
    var exception: Exception = null
    var count = 0
    var pushed = false
    while (!pushed && count <= 3) {
      try {
        receiver.store((arrayBuffer.asInstanceOf[ArrayBuffer[Byte]]).array())
        //receiver.store(arrayBuffer.asInstanceOf[ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      // Iterate through all the blocks and commit the last sequence number
      // TODO:  But what if these are out of order because Map doesn't guarantee order?
      // The Option here will filter out the nulls
      // TODO:  Figure this out
      //Option(lastSeqNumberOfGeneratedBlockMap.get(blockId))
      Option(lastCheckpointerOfGeneratedBlockMap.get(blockId))
        .foreach(lastCheckpointerOfGeneratedBlock => {
          //TODO:  checkpoint the sequence number of each block in the map
          //TODO:  how do we carry checkpointer over to here?
          lastCheckpointerOfGeneratedBlock.checkpoint()
        })

      // remove the blocks from the
      //TODO: figure this out
      //lastSeqNumberOfGeneratedBlockMap.remove(blockId)
      lastCheckpointerOfGeneratedBlockMap.remove(blockId)
    } else {
      receiver.stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * This method is called by the KCL when a batch of records is pulled from the Kinesis stream.
   * This is the record-processing bridge between the KCL's IRecordProcessor.processRecords()
   * and Spark Streaming's Receiver.store().
   *
   * @param batch list of records from the Kinesis stream shard
   * @param checkpointer used to update Kinesis when this batch has been processed/stored 
   *   in the DStream
   */
  override def processRecords(batch: List[Record], checkpointer: IRecordProcessorCheckpointer) {
    if (!receiver.isStopped()) {
      try {
        /*
         * Notes:  
         * 1) If we try to store the raw ByteBuffer from record.getData(), the Spark Streaming
         *    Receiver.store(ByteBuffer) attempts to deserialize the ByteBuffer using the
         *    internally-configured Spark serializer (kryo, etc).
         * 2) This is not desirable, so we instead store a raw Array[Byte] and decouple
         *    ourselves from Spark's internal serialization strategy.
         * 3) For performance, the BlockGenerator is asynchronously queuing elements within its
         *    memory before creating blocks.  This prevents the small block scenario, but requires
         *    that you register callbacks to know when a block has been generated and stored 
         *    (WAL is sufficient for storage) before can checkpoint back to the source.
         */
        // TODO: figure out how to get sequence number into metadata

        batch.foreach(record => 
          blockGenerator.addDataWithCallback(record.getData().array(), checkpointer)
          //receiver.store(record.getData().array())
        )
        
        logDebug(s"Stored:  Worker $workerId stored ${batch.size} records for shardId $shardId")
      } catch {
        case e: Throwable => {
          /*
           *  If there is a failure within the batch, the batch will not be checkpointed.
           *  This will potentially cause records since the last checkpoint to be processed
           *     more than once.
           */
          logError(s"Exception:  WorkerId $workerId encountered and exception while storing " +
              " or checkpointing a batch for workerId $workerId and shardId $shardId.", e)

          /* Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor.*/
          throw e
        }
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(s"Stopped:  The Spark KinesisReceiver has stopped for workerId $workerId" + 
          s" and shardId $shardId.  No more records will be processed.")
    }
  }

  /**
   * Kinesis Client Library is shutting down this Worker for 1 of 2 reasons:
   * 1) the stream is resharding by splitting or merging adjacent shards 
   *     (ShutdownReason.TERMINATE)
   * 2) the failed or latent Worker has stopped sending heartbeats for whatever reason 
   *     (ShutdownReason.ZOMBIE)
   *
   * @param checkpointer used to perform a Kinesis checkpoint for ShutdownReason.TERMINATE
   * @param reason for shutdown (ShutdownReason.TERMINATE or ShutdownReason.ZOMBIE)
   */
  override def shutdown(reason: ShutdownReason) {
    logInfo(s"Shutting down workerId $workerId with reason $reason")
    reason match {
      //TODO:  How do we handle a final checkpoint (if at all).  
      //       What if a checkpoint is in process from the callback?
      case ShutdownReason.TERMINATE => 
        //KinesisRecordProcessor.retryRandomBackoff(checkpointer.checkpoint(), 4, 100)

      /*
       * ZOMBIE Use Case.  NoOp.
       * No checkpoint because other workers may have taken over and already started processing
       *    the same records.
       * This may lead to records being processed more than once.
       */
      case ShutdownReason.ZOMBIE =>

      /* Unknown reason.  NoOp */
      case _ =>
    }

    if (blockGenerator != null) {
      // TODO:  Is this synchronous?
      //        Hopefully it is to allow graceful draining
      blockGenerator.stop()
      blockGenerator = null
    }

   /*
    * These should come last in case the blockGenerator still needs to fire callbacks
    * (TODO:  investigate concurrency properties of these callbacks)
    */
//    if (lastSeqNumberOfGeneratedBlockMap != null) {
//      lastSeqNumberOfGeneratedBlockMap = null
//    }
    if (lastCheckpointerOfGeneratedBlockMap != null) {
      lastCheckpointerOfGeneratedBlockMap = null
    }

    
//    latestSeqNumberWithinNotYetGeneratedBlock = null
    latestCheckpointerWithinNotYetGeneratedBlock = null

    shardId = null
  }
}

private[kinesis] object KinesisRecordProcessor extends Logging {
  /**
   * Retry the given amount of times with a random backoff time (millis) less than the
   *   given maxBackOffMillis
   *
   * @param expression expression to evalute
   * @param numRetriesLeft number of retries left
   * @param maxBackOffMillis: max millis between retries
   *
   * @return evaluation of the given expression
   * @throws Unretryable exception, unexpected exception,
   *  or any exception that persists after numRetriesLeft reaches 0
   */
  @annotation.tailrec
  def retryRandomBackoff[T](expression: => T, numRetriesLeft: Int, maxBackOffMillis: Int): T = {
    util.Try { expression } match {
      /* If the function succeeded, evaluate to x. */
      case util.Success(x) => x
      /* If the function failed, either retry or throw the exception */
      case util.Failure(e) => e match {
        /* Retry:  Throttling or other Retryable exception has occurred */
        case _: ThrottlingException | _: KinesisClientLibDependencyException if numRetriesLeft > 1
          => {
               val backOffMillis = Random.nextInt(maxBackOffMillis)
               Thread.sleep(backOffMillis)
               logError(s"Retryable Exception:  Random backOffMillis=${backOffMillis}", e)
               retryRandomBackoff(expression, numRetriesLeft - 1, maxBackOffMillis)
             }
        /* Throw:  Shutdown has been requested by the Kinesis Client Library.*/
        case _: ShutdownException => {
          logError(s"ShutdownException:  Caught shutdown exception, skipping checkpoint.", e)
          throw e
        }
        /* Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException => {
          logError(s"InvalidStateException:  Cannot save checkpoint to the DynamoDB table used" +
              s" by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        }
        /* Throw:  Unexpected exception has occurred */
        case _ => {
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
        }
      }
    }
  }
}
