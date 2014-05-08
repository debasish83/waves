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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.annotation.tailrec

/**
 * Minimalistic actor implementation without `become`.
 *
 * The atomic integer value signals whether we are currently running or scheduled to run on the given
 * ExecutionContext or whether we are "idle".
 * We start out in state SCHEDULED to protect ourselves from starting mailbox processing
 * before the object has been fully initialized.
 */
private[impl] abstract class SimpleActor(implicit ec: ExecutionContext) extends AtomicInteger
    with (AnyRef ⇒ Unit) with Runnable {
  private final val SCHEDULED = 0 // compile-time constant
  private final val IDLE = 1 // compile-time constant
  private final val Throughput = 5 // TODO: make configurable

  // TODO: upgrade to fast MPSC queue, e.g.
  // http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
  private[this] val mailbox = new ConcurrentLinkedQueue[AnyRef]

  def !(msg: AnyRef): Unit = {
    mailbox.offer(if (msg eq null) SimpleActor.NULL else msg)
    scheduleIfPossible()
  }

  final def run(): Unit =
    try {
      @tailrec def processMailbox(maxRemainingMessages: Int): Unit =
        if (maxRemainingMessages > 0) {
          val nextMsg = mailbox.poll()
          if (nextMsg ne null) {
            apply(if (nextMsg eq SimpleActor.NULL) null else nextMsg)
            processMailbox(maxRemainingMessages - 1)
          }
        }
      processMailbox(Throughput)
    } catch {
      case NonFatal(e) ⇒ // TODO: remove this debugging helper once we are stable
        print(s"ERROR in SimpleActor::apply: ")
        e.printStackTrace()
    } finally {
      startMessageProcessing()
    }

  private def scheduleIfPossible(): Unit =
    if (compareAndSet(IDLE, SCHEDULED)) {
      try ec.execute(this)
      catch {
        case NonFatal(e) ⇒
          set(IDLE)
          throw e
      }
    }

  // must be called at the end of the outermost constructor
  protected def startMessageProcessing(): Unit = {
    set(IDLE)
    if (!mailbox.isEmpty)
      scheduleIfPossible()
  }
}

object SimpleActor {
  private val NULL = new AnyRef
}