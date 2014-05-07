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
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.annotation.tailrec

/**
 * Minimalistic actor implementation without `become`.
 *
 * The atomic boolean value signals whether we are currently running or scheduled to run
 * on the given ExecutionContext. We start out with a value of `true` to protect ourselves
 * from starting mailbox processing before the object has been fully initialized.
 */
private[impl] abstract class SimpleActor(implicit ec: ExecutionContext) extends AtomicBoolean(true)
    with (AnyRef ⇒ Unit) with Runnable {
  private val Throughput = 5 // TODO: make configurable

  // TODO: if we can guarantee boundedness of the mailbox use optimized ringbuffer-based implementation
  // otherwise upgrade to "Gidenstam, Sundell and Tsigas"-like impl, e.g. ConcurrentArrayQueue from Jetty 9
  private[this] val mailbox = new ConcurrentLinkedQueue[AnyRef]

  def !(msg: AnyRef): Unit = {
    mailbox.offer(msg)
    scheduleIfPossible()
  }

  final def run(): Unit =
    try {
      @tailrec def processMailbox(maxRemainingMessages: Int): Unit =
        if (maxRemainingMessages > 0) {
          val nextMsg = mailbox.poll()
          if (nextMsg ne null) {
            apply(nextMsg)
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
    if (compareAndSet(false, true)) {
      try ec.execute(this)
      catch {
        case NonFatal(e) ⇒
          set(false)
          throw e
      }
    }

  // must be called at the end of the outermost constructor to 
  protected def startMessageProcessing(): Unit = {
    set(false)
    if (!mailbox.isEmpty)
      scheduleIfPossible()
  }
}