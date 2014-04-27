package akka.stream2.impl
package ops

import scala.annotation.tailrec
import scala.util.control.NonFatal

class Recover(f: Throwable ⇒ Seq[Any])(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  var requested = 0
  var errorOutput: List[Any] = _

  override def requestMore(elements: Int): Unit = {
    requested += elements
    if (errorOutput eq null) upstream.requestMore(requested)
    else if (requested == elements) drainErrorOutput()
  }

  override def onNext(element: Any): Unit = {
    downstream.onNext(element)
    requested -= 1
  }

  override def onError(cause: Throwable): Unit =
    try {
      errorOutput = f(cause).toList
      drainErrorOutput()
    } catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }

  @tailrec private def drainErrorOutput(): Unit =
    if (errorOutput.isEmpty) downstream.onComplete()
    else if (requested > 0) {
      downstream.onNext(errorOutput.head)
      requested -= 1
      errorOutput = errorOutput.tail
      drainErrorOutput()
    } // else wait for requestMore
}