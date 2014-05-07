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

import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, Promise, ExecutionContext }
import java.util.concurrent.{ SynchronousQueue, TimeUnit, ThreadPoolExecutor }

class SimpleActorSpec extends Specification with NoTimeConversions {
  import SimpleActorSpec._

  "The SimpleActor implementation" should {
    "provide memory consistency" in {
      val noOfActors = threads + 1
      val promises = Vector.fill(noOfActors)(Promise[Set[Long]]())
      val actors = Vector.tabulate(noOfActors)(ix ⇒ new ConsistencyCheckingActor(promises(ix)))

      for (i ← 0L until 100000L) actors.foreach(_ ! LongValue(i))
      for (a ← actors) { a ! 'done }

      val threadSets = Await.result(Future.sequence(promises.map(_.future)), 5.seconds)
      threadSets.map(_.size).min must be_>(threads) // each actor must have been scheduled across all threads
    }
  }

  step(executor.shutdown())
}

object SimpleActorSpec {
  val minThreads = 1
  val maxThreads = 2000
  val factor = 1.5d
  val threads = // Make sure we have more threads than cores
    math.min(math.max((Runtime.getRuntime.availableProcessors * factor).ceil.toInt, minThreads), maxThreads)

  val executor = new ThreadPoolExecutor(minThreads, maxThreads, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable])
  implicit val ec = ExecutionContext.fromExecutor(executor)

  class CacheMisaligned(var value: Long, var padding1: Long, var padding2: Long, var padding3: Int) //Vars, no final fences

  case class LongValue(l: Long)

  class ConsistencyCheckingActor(promise: Promise[Set[Long]]) extends SimpleActor {
    var threadSet = Set.empty[Long]
    var left = new CacheMisaligned(42, 0, 0, 0) //var
    var right = new CacheMisaligned(0, 0, 0, 0) //var
    var lastStep = -1L

    startMessageProcessing()

    def apply(msg: AnyRef): Unit = msg match {
      case LongValue(step) ⇒
        threadSet += Thread.currentThread().getId
        if (lastStep == (step - 1)) {
          val shouldBeFortyTwo = left.value + right.value
          if (shouldBeFortyTwo == 42) {
            left.value += 1
            right.value -= 1
            lastStep = step
          } else fail("Test failed: 42 failed")
        } else fail(s"Test failed: Last step $lastStep, this step $step")

      case 'done ⇒ promise.trySuccess(threadSet)
    }

    def fail(msg: String) = promise.tryFailure(new RuntimeException(msg))
  }
}