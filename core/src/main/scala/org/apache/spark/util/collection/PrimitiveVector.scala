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

import scala.reflect.ClassTag

/**
 * An append-only primitive vector for Long's.
 * This is its own separate class for convenient use in Java.
 * Note: DUPLICATE CODE ALERT!!
 */
private[spark] class PrimitiveLongVector(initialSize: Int = 64) {
  private var _numElements = 0
  private var _array: Array[Long] = _

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size.
  _array = new Array[Long](initialSize)

  def size: Int = _numElements

  def setSize(newSize: Int): Unit = {
    assert(newSize <= _numElements)
    _numElements = newSize
  }

  def append(value: Long): Unit = {
    if (_numElements == _array.length) {
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  def array: Array[Long] = _array

  /** Resizes the array, dropping elements if the total length decreases. */
  def resize(newLength: Int): Unit = {
    val newArray = new Array[Long](newLength)
    _array.copyToArray(newArray)
    _array = newArray
    if (newLength < _numElements) {
      _numElements = newLength
    }
  }
}

/**
 * An append-only, non-threadsafe, array-backed vector that is optimized for primitive types.
 */
private[spark]
class PrimitiveVector[@specialized(Long, Int, Double) V: ClassTag](initialSize: Int = 64) {
  private var _numElements = 0
  private var _array: Array[V] = _

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size.
  _array = new Array[V](initialSize)

  def apply(index: Int): V = {
    require(index < _numElements)
    _array(index)
  }

  def append(value: V): Unit = +=(value)

  def +=(value: V): Unit = {
    if (_numElements == _array.length) {
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  def capacity: Int = _array.length

  def length: Int = _numElements

  def size: Int = _numElements

  def iterator: Iterator[V] = new Iterator[V] {
    var index = 0
    override def hasNext: Boolean = index < _numElements
    override def next(): V = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = _array(index)
      index += 1
      value
    }
  }

  /** Gets the underlying array backing this vector. */
  def array: Array[V] = _array

  /** Trims this vector so that the capacity is equal to the size. */
  def trim(): PrimitiveVector[V] = resize(size)

  /** Resizes the array, dropping elements if the total length decreases. */
  def resize(newLength: Int): PrimitiveVector[V] = {
    val newArray = new Array[V](newLength)
    _array.copyToArray(newArray)
    _array = newArray
    if (newLength < _numElements) {
      _numElements = newLength
    }
    this
  }
}
