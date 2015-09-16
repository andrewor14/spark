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

package org.apache.spark.util.collection;

import java.util.ArrayList;

import org.apache.spark.sort.PairLong;


/**
 * An implementation of radix sort that buckets by bytes.
 *
 * The runtime should be roughly O(n * d), where n = num key, and d = num bytes per key.
 * This is not thread-safe and should not be used to sort two buffers in parallel.
 *
 * Note: This sorter currently does not support sorting buffers with a mixture of positive and
 * negative primitives, because it treats the inputs as bytes and does not know about signage.
 * E.g. -1 is treated as the max because it's all 1's, while 0 is treated as the min.
 *
 * HACK ALERT: This currently only works specifically with pair long arrays!!
 */
public class RadixSorter {

  private final SortDataFormat<PairLong, long[]> s;

  // Temp key holder for storing intermediate keys, reused many times
  private PairLong tempKeyHolder;

  public RadixSorter(SortDataFormat<PairLong, long[]> sortDataFormat) {
    this.s = sortDataFormat;
  }

  /**
   * Sort the given buffer using radix sort in place.
   *
   * This runs 10 phases, 1 for each key byte. Each phase takes exactly one pass over the data
   * and allocates 256 resizable long arrays. It is not the most memory efficient approach but
   * it takes the fewest number of passes over the data possible in Radix sort.
   */
  public void sort(long[] buffer) {
    int length = s.getLength(buffer);
    int numKeyBytes = s.getNumKeyBytes();
    tempKeyHolder = s.createTempKeyHolder();

    assert numKeyBytes > 0 : "Um need at least 1 byte to sort?";

    // Build up byte indices to process, least significant byte index first
    ArrayList<Integer> byteIndices = new ArrayList<Integer>();
    for (int i = 0; i < numKeyBytes; i++) {
      if (!s.keyBytesToIgnore().contains(i)) {
        byteIndices.add(0, i);
      }
    }

    // To avoid creating temp buffers all the time, we reuse two of these
    // across many phases, alternating which one to use for input vs output.
    PrimitiveLongChunkedVector[] tempBuffer1 = newVector();
    PrimitiveLongChunkedVector[] tempBuffer2 = newVector();
    PrimitiveLongChunkedVector[] inputBuffer = null;
    PrimitiveLongChunkedVector[] outputBuffer = null;

    // Sort the keys one byte at a time, starting from the least significant byte.
    // In the first phase, we read directly from the long array provided by the user.
    // In subsequent phases, we read from our temp buffers.
    for (int byteIndex : byteIndices) {
      if (inputBuffer == null) {
        // Read directly from the user provided long array
        assert outputBuffer == null : "output buffer shouldn't be initialized yet?";
        outputBuffer = tempBuffer1;
        inputBuffer = tempBuffer2;
        sortByByte(buffer, outputBuffer, length, byteIndex);
      } else {
        // Read from one of our temporary buffers.
        // If we used buffer 1 to store the output last time, use buffer 2 this time.
        // If we used buffer 2 to store the output last time, use buffer 1 this time.
        PrimitiveLongChunkedVector[] temp = inputBuffer;
        inputBuffer = outputBuffer;
        outputBuffer = temp;
        sortByByte(inputBuffer, outputBuffer, byteIndex);
      }
    }

    // Copy elements from temp buffer back to our original buffer
    assert outputBuffer != null : "output buffer was supposed to be initialized";
    int copyIndex = 0;
    for (int i = 0; i < outputBuffer.length; i += 1) {
      PrimitiveLongChunkedVector vec = outputBuffer[i];
      for (int j = 0; j < vec.size(); j += 1) {
        buffer[copyIndex] = vec.readNext();
        copyIndex++;
      }
    }
  }

  /**
   * Sort the input buffer by the specified byte index, to be called in phases 1.
   */
  private void sortByByte(
      long[] inputBuffer,
      PrimitiveLongChunkedVector[] outputBuffer,
      int numRecords,
      int byteIndex) {

    // Hash each key in a bucket
    for (int i = 0; i < numRecords; i++) {
      s.getKey(inputBuffer, i, tempKeyHolder);
      int bucketIndex = getBucketIndex(tempKeyHolder, byteIndex);
      outputBuffer[bucketIndex].append(tempKeyHolder._1());
      outputBuffer[bucketIndex].append(tempKeyHolder._2());
    }
  }

  /**
   * Sort the input buffer by the specified byte index, to be called in phases 2 and above.
   */
  private void sortByByte(
      PrimitiveLongChunkedVector[] inputBuffer,
      PrimitiveLongChunkedVector[] outputBuffer,
      int byteIndex) {
    assert inputBuffer.length == outputBuffer.length : "temp buffer sizes should not change";

    // Re-initialize output buffer before use
    for (PrimitiveLongChunkedVector vec : outputBuffer) {
      vec.reset();
    }

    // Hash each key in a bucket
    for (int i = 0; i < inputBuffer.length; i += 1) {
      PrimitiveLongChunkedVector vec = inputBuffer[i];
      assert vec.size() % 2 == 0 : "expected even number of Long's in the array.";
      for (int j = 0; j < vec.size(); j += 2) {
        tempKeyHolder.set_1(vec.readNext());
        tempKeyHolder.set_2(vec.readNext());
        int bucketIndex = getBucketIndex(tempKeyHolder, byteIndex);
        outputBuffer[bucketIndex].append(tempKeyHolder._1());
        outputBuffer[bucketIndex].append(tempKeyHolder._2());
      }
    }
  }

  /**
   * Create a new primitive long vector, to be called before each phase.
   */
  private PrimitiveLongChunkedVector[] newVector() {
    int size = Integer.parseInt(System.getProperty("spark.sort.radixVectorInitialSize", "2048"));
    PrimitiveLongChunkedVector[] tempBuffer = new PrimitiveLongChunkedVector[256];
    for (int x = 0; x < 256; x++) {
      tempBuffer[x] = new PrimitiveLongChunkedVector(size);
    }
    return tempBuffer;
  }

  private int getBucketIndex(PairLong key, int byteIndex) {
    return s.getKeyByte(key, byteIndex) & 0xff;
  }

}
