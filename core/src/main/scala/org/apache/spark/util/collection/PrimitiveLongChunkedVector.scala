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

import java.lang.Long

import scala.collection.mutable.ArrayBuffer


/**
 * An append-only vector for [[Long]]s that avoids copying when growing.
 * @param chunkSize number of [[Long]]s in each chunk
 */
class PrimitiveLongChunkedVector(chunkSize: Int = 1024) {
  private var numElements: Int = 0
  private var chunkIndex: Int = 0
  private var indexWithinChunk: Int = 0
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
    chunkIndex = 0
    indexWithinChunk = 0
  }

  /** Number of elements put in this vector. */
  def size: Int = numElements

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
    assert(indexWithinChunk <= chunkSize, "index within chunk is not within chunk?")
    if (indexWithinChunk == chunkSize) {
      chunkIndex += 1
      indexWithinChunk = 0
      if (chunkIndex < chunks.size) {
        // There might be an existing array if we called reset() before.
        // In this case, there's no need to allocate a new one.
      } else {
        chunks += new Array[Long](chunkSize)
      }
    }
    chunks(chunkIndex)(indexWithinChunk) = value
    indexWithinChunk += 1
    numElements += 1
  }

}
