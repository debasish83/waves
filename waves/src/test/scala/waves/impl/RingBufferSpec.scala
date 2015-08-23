package waves.impl

import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions

/*
 * Copyright (C) 2015 Debasish Das
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

class RingBufferSpec extends Specification with NoTimeConversions {
  "The RingBuffer implementation" should {
    "copy all elements from full buffer" in {
      val buf = new RingBuffer[Int](4)
      buf.tryEnqueue(2)
      buf.tryEnqueue(4)
      buf.tryEnqueue(3)
      buf.tryEnqueue(5)
      buf.dequeue()
      buf.dequeue()
      buf.tryEnqueue(1)
      buf.tryEnqueue(7)

      val expected = Array(3.0, 5.0, 1.0, 7.0)

      val dest = Array.fill[Int](4)(0)
      buf.copyToArray(dest, 0, 4)
      expected mustEqual dest
    }

    "copy 50% elements from full buffer" in {
      val buf = new RingBuffer[Int](4)
      buf.tryEnqueue(2)
      buf.tryEnqueue(4)
      buf.tryEnqueue(3)
      buf.tryEnqueue(5)
      buf.dequeue()
      buf.dequeue()
      buf.tryEnqueue(1)
      buf.tryEnqueue(6)

      val dest = Array.fill[Int](2)(0)
      buf.copyToArray(dest, 2, 2)
      dest mustEqual Array(1, 6)
    }

    "copy 50% elements from partially full buffer" in {
      val buf = new RingBuffer[Int](8)
      buf.tryEnqueue(2)
      buf.tryEnqueue(4)
      buf.tryEnqueue(3)
      buf.tryEnqueue(5)
      buf.dequeue()
      buf.dequeue()
      buf.tryEnqueue(1)
      buf.tryEnqueue(6)
      val dest = Array.fill[Int](2)(0)
      buf.copyToArray(dest, 2, 2)
      dest mustEqual Array(1, 6)
    }

    "copy 67% elements from partially full buffer" in {
      val buf = new RingBuffer[Int](8)
      buf.tryEnqueue(2)
      buf.tryEnqueue(4)
      buf.tryEnqueue(3)
      buf.tryEnqueue(5)
      buf.dequeue()
      buf.dequeue()
      buf.tryEnqueue(1)
      buf.tryEnqueue(6)
      buf.dequeue()
      buf.dequeue()
      buf.tryEnqueue(7)
      buf.tryEnqueue(8)

      val dest = Array.fill[Int](3)(0)
      buf.copyToArray(dest, 1, 3)
      dest mustEqual Array(6, 7, 8)
    }
  }
}

