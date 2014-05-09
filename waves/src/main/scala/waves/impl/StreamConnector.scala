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

private[impl] class StreamConnector extends Upstream with Downstream {
  import StreamConnector._

  protected var upstream: Upstream = BlackHoleUpstream
  protected var downstream: Downstream = BlackHoleDownstream

  def connectUpstream(upstream: Upstream): Unit = this.upstream = upstream
  def connectDownstream(downstream: Downstream): Unit = this.downstream = downstream

  def upstreamConnected: Boolean = upstream ne BlackHoleUpstream
  def downstreamConnected: Boolean = downstream ne BlackHoleDownstream

  def requestMore(elements: Int) = upstream.requestMore(elements)
  def cancel() = {
    val up = upstream
    disconnect()
    up.cancel()
  }

  def onNext(element: Any) = downstream.onNext(element)
  def onComplete() = {
    val down = downstream
    disconnect()
    down.onComplete()
  }
  def onError(cause: Throwable) = {
    val down = downstream
    disconnect()
    down.onError(cause)
  }

  private def disconnect(): Unit = {
    upstream = BlackHoleUpstream
    downstream = BlackHoleDownstream
  }
}

private object StreamConnector {
  private case object BlackHoleUpstream extends Upstream {
    def requestMore(elements: Int) = ()
    def cancel() = ()
  }

  private case object BlackHoleDownstream extends Downstream {
    def onNext(element: Any) = ()
    def onComplete() = ()
    def onError(cause: Throwable) = ()
  }
}

private[impl] final class UpstreamConnector extends StreamConnector {
  private[this] var collector = new CollectingUpstream
  upstream = collector

  override def connectUpstream(upstream: Upstream): Unit =
    if (this.upstream eq collector) {
      this.upstream = upstream
      collector.flushTo(upstream)
      collector = null // free memory and signal that we have flushed
    } else if (upstreamConnected) {
      throw new IllegalStateException(s"Cannot subscribe to second upstream $upstream, already connected to ${this.upstream}")
    } else if (collector ne null) {
      upstream.cancel() // the upstream has been cancelled *before* we were even connected
    } else {
      throw new IllegalStateException(s"Cannot subscribe to upstream $upstream, already subscribed before and cancelled")
    }

  private class CollectingUpstream extends Upstream {
    private[this] var requested = 0
    def requestMore(elements: Int) = requested += elements
    def cancel() = ()
    def flushTo(upstream: Upstream): Unit = if (requested > 0) upstream.requestMore(requested)
  }
}

private[impl] final class DownstreamConnector extends StreamConnector {
  private[this] var collector = new CollectingDownstream
  downstream = collector

  override def connectDownstream(downstream: Downstream): Unit =
    if (this.downstream eq collector) {
      this.downstream = downstream
      collector.flushTo(downstream)
      collector = null // free memory and signal that we have flushed
    } else if (downstreamConnected) {
      throw new IllegalStateException(s"Cannot attach 2nd downstream $downstream, downstream ${this.downstream} is already attached")
    } else if (collector ne null) {
      // the downstream has been completed *before* we were even connected, so complete by flushing
      collector.flushTo(downstream)
    } else {
      throw new IllegalStateException(s"Cannot attach downstream $downstream, another downstream was already attached and completed")
    }

  private class CollectingDownstream extends Downstream {
    private[this] var termination: Option[Throwable] = _
    def onNext(element: Any) = throw new IllegalStateException // unrequested onNext
    def onComplete() = termination = None
    def onError(cause: Throwable) = termination = Some(cause)
    def flushTo(downstream: Downstream): Unit =
      termination match {
        case null        ⇒ // nothing to do
        case None        ⇒ downstream.onComplete()
        case Some(error) ⇒ downstream.onError(error)
      }
  }
}