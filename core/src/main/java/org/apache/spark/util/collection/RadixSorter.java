package org.apache.spark.util.collection;

import java.util.ArrayList;

/**
 * An implementation of radix sort that buckets by bytes.
 *
 * The runtime should be roughly O(n * d), where n = num key, and d = num bytes per key.
 * This is not thread-safe and should not be used to sort two buffers in parallel.
 *
 * Note: This sorter currently does not support sorting buffers with a mixture of positive and
 * negative primitives, because it treats the inputs as bytes and does not know about signage.
 * E.g. -1 is treated as the max because it's all 1's, while 0 is treated as the min.
 */
public class RadixSorter<K, Buffer> {

  private final SortDataFormat<K, Buffer> s;

  // Number of bytes in key K, also the number of buckets
  private final int numKeyBytes;

  // Number of items in each bucket, reused across many phases
  private final int[] counts;

  // Temp key holder for storing intermediate keys, reused many times
  private K tempKeyHolder;

  public RadixSorter(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
    this.numKeyBytes = s.getNumKeyBytes();
    this.counts = new int[256]; // this supports at most 2^32 duplicate bytes per key

    assert numKeyBytes > 0 : "Um need at least 1 byte to sort?";
  }

  /**
   * Sort the given buffer using radix sort.
   * Note that the contents of the input buffer will be destroyed in place.
   */
  public Buffer sort(Buffer input) {
    int length = s.getLength(input);
    Buffer tempBuffer = s.allocate(length);
    tempKeyHolder = s.createTempKeyHolder();

    // To avoid copying elements back and forth between the two
    // buffers at the end of each phase, just swap the pointers
    Buffer inputBuffer = input;
    Buffer outputBuffer = tempBuffer;

    // Build up byte indices to process
    ArrayList<Integer> byteIndices = new ArrayList<Integer>();
    for (int i = 0; i < numKeyBytes; i++) {
      if (!s.keyBytesToIgnore().contains(i)) {
        byteIndices.add(0, i);
      }
    }

    // Sort the keys one byte at a time, starting from the least significant byte
    int lastByteIndex = byteIndices.get(byteIndices.size() - 1);
    for (int byteIndex : byteIndices) {
      sortByByte(inputBuffer, outputBuffer, length, byteIndex);
      if (byteIndex != lastByteIndex) {
        // If there is a next phase, we use our output buffer as the new source of data, so we
        // no longer need our input buffer and can reuse it to store the output of the next phase.
        Buffer temp = inputBuffer;
        inputBuffer = outputBuffer;
        outputBuffer = temp;
      }
    }

    return outputBuffer;
  }

  /**
   * Sort the contents of the input buffer by a specific byte.
   * This fills the output buffers with the keys in the resulting order.
   */
  private void sortByByte(Buffer inputBuffer, Buffer outputBuffer, int length, int byteIndex) {

    // Hash each key by its byte at `byteIndex` and fill in the counts array
    for (int i = 0; i < length; i++) {
      s.getKey(inputBuffer, i, tempKeyHolder);
      int bucketIndex = getBucketIndex(tempKeyHolder, byteIndex);
      counts[bucketIndex]++;
    }

    // Convert the counts to accumulated values to use as indices later
    // E.g. [2, 2, 1, 4, 0, 3] -> [2, 4, 5, 9, 9, 12]
    for (int j = 1; j < counts.length; j++) {
      counts[j] += counts[j - 1];
    }

    // Fill in temp buffer using the new ordering specified by the counts
    // Note: we must start at the back because the previous phases have already put the keys
    // in increasing order by the less significant bytes. Since we put the keys from back to
    // front we should continue to respect this partial ordering.
    for (int i = length - 1; i >= 0; i--) {
      s.getKey(inputBuffer, i, tempKeyHolder);
      int bucketIndex = getBucketIndex(tempKeyHolder, byteIndex);
      counts[bucketIndex]--;
      s.putKey(outputBuffer, counts[bucketIndex], tempKeyHolder);
    }

    // Zero out the counts array
    for (int j = 0; j < counts.length; j++) {
      counts[j] = 0;
    }
  }

  private int getBucketIndex(K key, int byteIndex) {
    return s.getKeyByte(key, byteIndex) & 0xff;
  }

}
