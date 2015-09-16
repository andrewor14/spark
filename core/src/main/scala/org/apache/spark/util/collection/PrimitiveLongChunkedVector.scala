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

package org.apache.spark.util.collection

import scala.collection.mutable.ArrayBuffer


/**
 * An append-only vector for [[Long]]s that avoids copying when growing.
 * @param chunkSize number of [[Long]]s in each chunk
 */
class PrimitiveLongChunkedVector(chunkSize: Int = 1024) {
  private var numElements: Int = 0
  private var writeChunkIndex: Int = 0
  private var writeIndexWithinChunk: Int = 0
  private var readChunkIndex: Int = 0
  private var readIndexWithinChunk: Int = 0
  private val chunks: ArrayBuffer[Array[Long]] = {
    val buf = new ArrayBuffer[Array[Long]]
    buf += new Array[Long](chunkSize)
    buf
  }

  require(chunkSize > 0, "chunk size must be greater than 0")

  reset()

  /**
   * Reset the variables describing this vector.
   * Note: this does not release the memory resources held just yet.
   */
  def reset(): Unit = {
    numElements = 0
    writeChunkIndex = 0
    writeIndexWithinChunk = 0
    readChunkIndex = 0
    readIndexWithinChunk = 0
  }

  /** Number of elements put in this vector. */
  def size: Int = numElements

  // HACK ALERT: do not use iterator here to avoid boxing and stuff
  def readNext(): Long = {
    val res = chunks(readChunkIndex)(readIndexWithinChunk)
    readIndexWithinChunk += 1
    if (readIndexWithinChunk == chunkSize) {
      readChunkIndex += 1
      readIndexWithinChunk = 0
    }
    res
  }

  /** Return the [[Long]] stored at the specified index. */
  def apply(index: Int): Long = {
    if (index >= numElements) {
      throw new IllegalArgumentException(s"requested index $index must be < $numElements")
    }
    val theChunkIndex = index / chunkSize
    val theIndexWithinChunk = index % chunkSize
    chunks(theChunkIndex)(theIndexWithinChunk)
  }

  /** Add a new [[Long]] to this vector, allocating a new chunk if necessary */
  def append(value: Long): Unit = {
    assert(writeIndexWithinChunk <= chunkSize, "index within chunk is not within chunk?")
    if (writeIndexWithinChunk == chunkSize) {
      writeChunkIndex += 1
      writeIndexWithinChunk = 0
      if (writeChunkIndex < chunks.size) {
        // There might be an existing array if we called reset() before.
        // In this case, there's no need to allocate a new one.
      } else {
        chunks += new Array[Long](chunkSize)
      }
    }
    chunks(writeChunkIndex)(writeIndexWithinChunk) = value
    writeIndexWithinChunk += 1
    numElements += 1
  }

}
