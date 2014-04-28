package akka.stream2.impl
package ops

import scala.annotation.tailrec

class Buffer(size: Int)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {
  import Buffer._

  val queue = new RingBuffer[Any](size)
  var requested = 0
  var pendingFromUpstream = 0

  deliver()

  override def requestMore(elements: Int): Unit = {
    requested += elements
    if (requested == elements) deliver()
  }

  override def onNext(element: Any) = {
    pendingFromUpstream -= 1
    enqueue(element)
  }
  override def onComplete() = enqueue(Complete)
  override def onError(cause: Throwable) = enqueue(Error(cause))

  private def enqueue(value: Any): Unit =
    if (queue.tryEnqueue(value))
      deliver()
    else throw new IllegalStateException

  @tailrec
  private def deliver(): Unit =
    if (queue.isEmpty) {
      if (pendingFromUpstream == 0) {
        // strategy: we only request from upstream if the queue is empty and there are no elements pending anymore,
        // so we trade off a little bit of latency (once every size elements)
        // against higher throughput through fewer requestMore calls
        pendingFromUpstream = size - 1 // we need to keep one slot available for a potential `Complete` or `Error` event
        upstream.requestMore(pendingFromUpstream)
      }
    } else queue.peek match {
      case Complete     ⇒ downstream.onComplete()
      case Error(cause) ⇒ downstream.onError(cause)
      case element if requested > 0 ⇒
        queue.drop()
        downstream.onNext(element)
        requested -= 1
        deliver()
      case _ ⇒ // wait for requestMore
    }
}

object Buffer {
  private case object Complete
  private final case class Error(cause: Throwable)
}