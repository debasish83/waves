package akka.stream2

import scala.concurrent.{ Future, ExecutionContext }
import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

class FutureProducer[T](future: Future[T])(implicit executor: ExecutionContext) //FIXME Remove defaults in code
  extends AbstractSyncedProducer[T](initialBufferSize = 1, maxBufferSize = 1) {

  @volatile private[this] var requested = false

  protected def requestFromUpstream(elements: Int): Unit =
    if (elements > 0 && !requested) {
      requested = true
      future.onComplete(complete)
    }

  @tailrec private def complete(completion: Try[T]): Unit =
    if (locked.compareAndSet(false, true)) {
      try {
        completion match {
          case Success(value) ⇒ pushToDownstream(value)
          case Failure(error) ⇒ abortDownstream(error)
        }
      } finally locked.set(false)
    } else complete(completion)

  protected def cancelUpstream(): Unit = ()
  protected def shutdown(): Unit = ()
}
