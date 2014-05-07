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

/**
 * Minimalistic actor implementation without `become`.
 *
 * The atomic boolean value signals whether we are currently running or scheduled to run
 * on the given ExecutionContext.
 */
abstract class SimpleActor(implicit ec: ExecutionContext) extends AtomicBoolean with Runnable {
  type Receive = Any ⇒ Unit

  // TODO: if we can guarantee boundedness of the mailbox use optimized ringbuffer-based implementation
  // otherwise upgrade to "Gidenstam, Sundell and Tsigas"-like impl, e.g. ConcurrentArrayQueue from Jetty 9
  private[this] val mailbox = new ConcurrentLinkedQueue[Any]

  val receive: Receive

  def !(msg: Any): Unit = {
    mailbox.offer(msg)
    schedule()
  }

  final def run(): Unit =
    try {
      if (get) // memory barrier and protection against semi-failed schedulings (exception from `ec.execute(this)`)
        receive(mailbox.poll())
    } catch {
      case NonFatal(e) ⇒ // TODO: remove this debugging helper once we are stable
        println(s"ERROR in SimpleActor::receive: " + e)
        e.printStackTrace()
    } finally {
      set(false)
      if (!mailbox.isEmpty)
        schedule()
    }

  private def schedule(): Unit =
    if (compareAndSet(false, true)) {
      try ec.execute(this)
      catch {
        case NonFatal(e) ⇒
          set(false)
          throw e
      }
    }
}