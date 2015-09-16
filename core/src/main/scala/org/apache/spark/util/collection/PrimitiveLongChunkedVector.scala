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

import org.apache.spark.sort.SortUtils


/**
 * An append-only vector for [[Long]]s that avoids copying when growing.
 * @param chunkSize size of each chunk, in bytes
 */
class PrimitiveLongChunkedVector(chunkSize: Long, numTotalChunks: Int) {
  import PrimitiveLongChunkedVector._
  import SortUtils.UNSAFE

  private var numChunks = 1
  private var numElements: Int = 0
  private var writeChunkIndex: Int = 0
  private var writeIndexWithinChunk: Int = 0
  private var readChunkIndex: Int = 0
  private var readIndexWithinChunk: Int = 0

  private val numLongsPerChunk = chunkSize / LONG_SIZE
  private val chunkAddresses: Array[Long] = {
    val arr = new Array[Long](numTotalChunks)
    arr(0) = UNSAFE.allocateMemory(chunkSize)
    arr
  }

  require(numLongsPerChunk > 0, "chunk size must be greater than 0")
  require(chunkSize % LONG_SIZE == 0, "chunk size must be word-aligned")

  reset()

  def this() {
    this(4L * 1000 * 1000, 64)
  }

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
    val addr = chunkAddresses(readChunkIndex) + readIndexWithinChunk * LONG_SIZE
    val res = UNSAFE.getLong(addr)
    readIndexWithinChunk += 1
    if (readIndexWithinChunk == numLongsPerChunk) {
      readChunkIndex += 1
      readIndexWithinChunk = 0
    }
    res
  }

  /** Add a new [[Long]] to this vector, allocating a new chunk if necessary */
  def append(value: Long): Unit = {
    assert(writeIndexWithinChunk <= numLongsPerChunk, "index within chunk is not within chunk?")
    if (writeIndexWithinChunk == numLongsPerChunk) {
      writeChunkIndex += 1
      writeIndexWithinChunk = 0
      assert(writeChunkIndex <= numChunks)
      // There might be an existing array if we called reset() before.
      // In this case, there's no need to allocate a new one.
      if (writeChunkIndex == numChunks) {
        chunkAddresses(numChunks) = UNSAFE.allocateMemory(chunkSize)
        numChunks += 1
      }
    }
    val addr = chunkAddresses(writeChunkIndex) + writeIndexWithinChunk * LONG_SIZE
    UNSAFE.putLong(addr, value)
    writeIndexWithinChunk += 1
    numElements += 1
  }

  /** Free all the memory used by this vector. */
  def free(): Unit = {
    var i = 0
    while (i < numChunks) {
      val addr = chunkAddresses(i)
      assert(addr > 0)
      UNSAFE.freeMemory(addr)
      i += 1
    }
  }

}

private object PrimitiveLongChunkedVector {
  val LONG_SIZE = 8 // bytes
}
