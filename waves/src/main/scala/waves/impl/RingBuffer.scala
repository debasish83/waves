/*
 * Copyright (C) 2014 Mathias Doenitz
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
}
