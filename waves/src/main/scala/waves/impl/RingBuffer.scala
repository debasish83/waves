/*
 * Copyright (C) 2015 Mathias Doenitz, Debasish Das
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package waves.impl

import scala.reflect.ClassTag

/**
 * Simple efficient ring-buffer with a power-of-2 size.
 */
class RingBuffer[T: ClassTag](val size: Int) {
  require(Integer.lowestOneBit(size) == size, "size must be a power of 2")

  private[this] val array = new Array[T](size)

  /*
   * Two counters counting the number of elements ever written and read
   * wrap-around is handled by always looking at differences or masked values
   */
  private[this] var writeIx = 0
  private[this] var readIx = 0 // the "oldest" of all read cursor indices, i.e. the one that is most behind

  private def mask = size - 1

  /**
   * The number of elements currently in the buffer.
   */
  def count: Int = writeIx - readIx

  def isEmpty: Boolean = count == 0

  def nonEmpty: Boolean = count > 0

  /**
   * Tries to put the given value into the buffer and returns true if this was successful.
   */
  def tryEnqueue(value: T): Boolean =
    if (count < size) {
      array(writeIx & mask) = value
      writeIx += 1
      true
    } else false

  /**
   * Reads and removes the next value from the buffer.
   * If the buffer is empty the method throws a NoSuchElementException.
   */
  def dequeue(): T =
    if (count > 0) {
      val result = array(readIx & mask)
      readIx += 1
      result
    } else throw new NoSuchElementException

  /**
   * Reads the next value from the buffer without removing it
   * If the buffer is empty the method throws a NoSuchElementException.
   */
  def peek: T =
    if (count > 0) array(readIx & mask)
    else throw new NoSuchElementException

  /**
   * Removes the next value from the buffer without reading it first.
   * If the buffer is empty the method throws a NoSuchElementException.
   */
  def drop(): Unit =
    if (count > 0) readIx += 1
    else throw new NoSuchElementException

  /**
   * Copies selected values from the RingBuffer to an array through at most 2
   * native System.arraycopy for efficiency. Fills the given array `xs` starting
   * at elements of RingBuffer from index `start` with at most `len` values
   * from the RingBuffer storage. Copying will stop once either the end of the
   * if the index `start` and number of elements specified by `len` does not exist
   * in RingBuffer internal storage.
   *
   *  @param  xs     the array to fill.
   *  @param  start  the starting index from RingBuffer
   *  @param  len    the number of elements to copy.
   *
   */
  def copyToArray(xs: Array[T], start: Int, len: Int): Unit = {
    require(xs.length == len, s"insufficient size $len to copy")
    require(start >= 0 && start < count, s"start index $start out of bounds $count")
    require(start + len <= count, s"copy elements $len more than buffer elements $count")

    val startIndex = (readIx + start) & mask
    //TO DO: Can we clean Math.min and if (startIndex) ?
    val forwardLength = Math.min(size - startIndex, len)
    System.arraycopy(array, startIndex, xs, 0, forwardLength)
    if (startIndex > 0) {
      val wrapLength = len - forwardLength
      System.arraycopy(array, 0, xs, forwardLength, wrapLength)
    }
  }
}

