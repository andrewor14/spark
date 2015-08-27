package org.apache.spark.sort

import java.nio.ByteBuffer

import org.apache.spark.util.collection.{OldRadixSorter, TimSorter, RadixSorter, SortDataFormat}


object SortUtils {

  final val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }

  final val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  /**
   * A class to hold information needed to run sort within each partition.
   *
   * @param capacity number of records the buffer can support. Each record is 100 bytes.
   */
  final class SortBuffer(capacity: Long) {
    require(capacity <= Int.MaxValue)

    val IO_BUF_LEN = 8 * 1024 * 1024

    /** size of the buffer, starting at [[address]] */
    val len: Long = capacity * DaytonaSort.RECORD_SIZE

    /** address pointing to a block of memory off heap */
    var address: Long = {
      val blockSize = capacity * DaytonaSort.RECORD_SIZE
      val blockAddress = UNSAFE.allocateMemory(blockSize)
      blockAddress
    }

    def releaseMapSideBuffer() {
      if (address != 0) {
        UNSAFE.freeMemory(address)
        pointers = null
        address = 0L
      }
    }


    // Each chunk should be 512MB
    val CHUNK_SIZE = 512L * 1000 * 1000
    // Each item in this array represents the starting address of a chunk
    val chunkBegin = new Array[Long](32)
    // Each item in this array represents the ending address of the corresponding chunk
    val chunkEnds = new Array[Long](32)
    var currentNumChunks = 0

    def currentChunkBaseAddress: Long = {
      assert(currentNumChunks > 0, "no chunk allocated yet")
      chunkBegin(currentNumChunks - 1)
    }

    def allocateNewChunk() {
      chunkBegin(currentNumChunks) = UNSAFE.allocateMemory(CHUNK_SIZE)
      currentNumChunks += 1
      println(s"allocating new chunk at $currentNumChunks")
    }

    def markLastChunkUsage(len: Long) {
      assert(currentNumChunks > 0, "no chunk allocated yet")
      chunkEnds(currentNumChunks - 1) = chunkBegin(currentNumChunks - 1) + len
    }

    def freeChunks() {
      var i = 0
      while (i < currentNumChunks) {
        UNSAFE.freeMemory(chunkBegin(i))
        i += 1
      }
      currentNumChunks = 0
    }

    /**
     * A dummy direct buffer. We use this in a very unconventional way. We use reflection to
     * change the address of the offheap memory to our large buffer, and then use channel read
     * to directly read the data into our large buffer.
     *
     * i.e. the 4MB allocated here is not used at all. We are only the 4MB for tracking.
     */
    val ioBuf: ByteBuffer = ByteBuffer.allocateDirect(IO_BUF_LEN)

    /** list of pointers to each block, used for sorting. */
    var pointers: Array[Long] = new Array[Long](capacity.toInt)

    /** an array of 2 * capacity longs that we can use for records holding our keys */
    var keys: Array[Long] = new Array[Long](2 * capacity.toInt)

    private[this] val ioBufAddressField = {
      val f = classOf[java.nio.Buffer].getDeclaredField("address")
      f.setAccessible(true)
      f
    }

    /** Return the memory address of the memory the [[ioBuf]] points to. */
    def ioBufAddress: Long = ioBufAddressField.getLong(ioBuf)

    def setIoBufAddress(addr: Long) = {
      ioBufAddressField.setLong(ioBuf, addr)
    }
  }  // end of SortBuffer

  /** A thread local variable storing a pointer to the buffer allocated off-heap. */
  val sortBuffers = new ThreadLocal[SortBuffer]

  def sortWithKeysUsingChunks(sortBuf: SortBuffer, numRecords: Int) {
    // Fill in the keys array
    sortBuf.keys = new Array[Long](numRecords * 2)
    val keys = sortBuf.keys
    var i = 0
    var chunkIndex = 0
    var indexWithinChunk = 0
    var addr: Long = sortBuf.chunkBegin(0)
    while (i < numRecords) {
      assert(addr >= sortBuf.chunkBegin(chunkIndex) && addr <= sortBuf.chunkEnds(chunkIndex),
        s"addr $addr, begin ${sortBuf.chunkBegin(chunkIndex)}, end ${sortBuf.chunkEnds(chunkIndex)}")
      assert(chunkIndex < sortBuf.currentNumChunks,
        s"chunkindex $chunkIndex should < ${sortBuf.currentNumChunks}")
      if (addr >= sortBuf.chunkEnds(chunkIndex)) {
        chunkIndex += 1
        indexWithinChunk = 0
        addr = sortBuf.chunkBegin(chunkIndex)
      }
      assert(chunkIndex < sortBuf.currentNumChunks,
        s"chunkindex $chunkIndex should < ${sortBuf.currentNumChunks}")
      val headBytes: Long = // First 7 bytes
        java.lang.Long.reverseBytes(UNSAFE.getLong(addr)) >>> 8
      val tailBytes: Long = // Last 3 bytes
        java.lang.Long.reverseBytes(UNSAFE.getLong(addr + 7)) >>> (8 * 5)
      keys(2 * i) = headBytes

      // Use the lower 23 bits for index within a chunk, and bit 23 to bit 31 for chunkIndex
      keys(2 * i + 1) = (tailBytes << 32) | (chunkIndex.toLong << 23) | indexWithinChunk.toLong
      addr += DaytonaSort.RECORD_SIZE
      i += 1
      indexWithinChunk += 1
    }

    // Note: on the reduce side, we ALWAYS want to use TimSort
    // because the keys should be partially sorted already.
    sortLongPairs(keys, algorithm = "tim")
  }

  // Sort a range of a SortBuffer using only the keys, then update the pointers field to match
  // sorted order. Unlike the other sort methods, this copies the keys into an array of Longs
  // (with 2 Longs per record in the buffer to capture the 10-byte key and its index) and sorts
  // them without having to look up random locations in the original data on each comparison.
  def sortWithKeys(sortBuf: SortBuffer, numRecords: Int) {
    val keys = sortBuf.keys
    val pointers = sortBuf.pointers
    val baseAddress = sortBuf.address
    var recordAddress = baseAddress

    // Fill in the keys array
    var i = 0
    while (i < numRecords) {

      // We store each 10-byte key in two Longs. The first Long stores the first 7 bytes,
      // and the second Long stores the remaining 3 bytes. We store only 7 bytes in the first
      // Long because Java does not support unsigned primitives, and so we must leave the first
      // byte of each Long empty to avoid comparing negative keys.

      // Note: UNSAFE.getLong returns the 8 bytes in reverse order
      // e.g. 11L -> 00001011 00000000 00000000 00000000 00000000 00000000 00000000 00000000

      val firstLong = java.lang.Long.reverseBytes(UNSAFE.getLong(recordAddress)) >>> 8
      val secondLong = java.lang.Long.reverseBytes(UNSAFE.getLong(recordAddress + 7)) >>> (8 * 5)
      keys(2 * i) = firstLong
      // The second long only contains 3 bytes of the keys, which are stored in the upper 4 bytes.
      // The lower 4 bytes are used to store the record index (an integer) within a partition.
      keys(2 * i + 1) = (secondLong << 32) | i.toLong
      recordAddress += DaytonaSort.RECORD_SIZE
      i += 1
    }

    // Sort it
    sortLongPairs(keys)

    // Fill back the pointers array
    i = 0
    while (i < numRecords) {
      pointers(i) = baseAddress + (keys(2 * i + 1) & 0xFFFFFFFFL) * DaytonaSort.RECORD_SIZE
      i += 1
    }
  }

  /** Actually sort the pair long array, using a configurable sorting algorithm. */
  def sortLongPairs(keys: Array[Long], algorithm: String = sortAlgorithm): Unit = {
    val sortFormat = new LongPairArraySorter
    algorithm match {
      case "tim" => new TimSorter(sortFormat).sort(keys, 0, keys.size / 2, longPairOrdering)
      case "radix" => new OldRadixSorter(sortFormat).sort(keys)
      case "radix2" => new RadixSorter(sortFormat).sort(keys)
      case bad => throw new IllegalArgumentException("Unknown sorting algorithm: " + bad)
    }
  }

  private[spark] val sortAlgorithm: String = sys.props.getOrElse("spark.sort.algorithm", "tim")

  private[spark] val longPairOrdering = new LongPairOrdering

}

final class PairLong(var _1: Long, var _2: Long) {
  override def toString: String = "(" + _1 + ", " + _2 + ")"

  // For Java.
  def set_1(l: Long): Unit = { _1 = l }
  def set_2(l: Long): Unit = { _2 = l }
}

private[spark] final class LongPairArraySorter extends SortDataFormat[PairLong, Array[Long]] {

  override protected def getKey(data: Array[Long], pos: Int) = ???

  override protected def createTempKeyHolder(): PairLong = new PairLong(0L, 0L)

  /** Return the sort key for the element at the given index. */
  override protected def getKey(data: Array[Long], pos: Int, reuse: PairLong): PairLong = {
    reuse._1 = data(2 * pos)
    reuse._2 = data(2 * pos + 1)
    reuse
  }

  /** Put a key in the specified index in our buffer. */
  override protected def putKey(data: Array[Long], pos: Int, reuse: PairLong): Unit = {
    data(2 * pos) = reuse._1
    data(2 * pos + 1) = reuse._2
  }

  override protected def getLength(data: Array[Long]): Int = {
    assert(data.length % 2 == 0, "input array size should be even...")
    data.length / 2
  }

  override protected def getKeyByte(key: PairLong, nthByte: Int): Byte = {
    assert(nthByte >= 0 && nthByte < 16, "requested invalid byte: " + nthByte)
    val mask = 255 // to get the least significant byte
    val long = if (nthByte < 8) key._1 else key._2
    val longByteIndex = nthByte % 8
    val shift = (7 - longByteIndex) * 8
    // e.g. nthByte = 14, longByteIndex = 6, shift = 1
    // in other words, we want the 7th byte of the second Long, so we shift it right by 1 byte
    ((long >> shift) & mask).toByte
  }

  override protected def getNumKeyBytes: Int = 16

  // This is highly-specific to our input encoding!
  // We have 2 longs, of which the following bytes actually store the keys (denoted by 'k'):
  // (_, k, k, k, k, k, k, k) (_, k, k, k, _, _, _, _)
  override def keyBytesToIgnore: Set[Int] = Set(0, 8, 12, 13, 14, 15)

  /** Swap two elements. */
  override protected def swap(data: Array[Long], pos0: Int, pos1: Int) {
    var tmp = data(2 * pos0)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos1) = tmp
    tmp = data(2 * pos0 + 1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1 + 1) = tmp
  }

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  override protected def copyElement(src: Array[Long], srcPos: Int,
                                     dst: Array[Long], dstPos: Int) {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   */
  override protected def copyRange(src: Array[Long], srcPos: Int,
                                   dst: Array[Long], dstPos: Int, length: Int) {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   */
  override protected def allocate(length: Int): Array[Long] = new Array[Long](2 * length)
}

private[spark] final class LongPairOrdering extends Ordering[PairLong] {
  override def compare(left: PairLong, right: PairLong): Int = {
    val c1 = java.lang.Long.compare(left._1, right._1)
    if (c1 != 0) {
      c1
    } else {
      java.lang.Long.compare(left._2, right._2)
    }
  }
}
