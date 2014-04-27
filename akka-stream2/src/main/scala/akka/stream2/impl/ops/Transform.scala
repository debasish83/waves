package akka.stream2
package impl
package ops

import scala.annotation.tailrec
import Operation.Transformer

class Transform(transformer: Transformer[Any, Any])(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Stateful {

  var requested = 0
  val startBehavior = behavior

  def initialBehavior: Behavior =
    new Behavior with (() ⇒ Unit) {
      override def requestMore(elements: Int) = {
        requested += elements
        if (requested == elements) upstream.requestMore(1)
      }

      override def onNext(element: Any): Unit = {
        val output =
          try transformer.onNext(element)
          catch {
            case t: Throwable ⇒
              onError(t)
              upstream.cancel()
              return
          }
        deliver(output.toList, this)
      }

      override def onComplete(): Unit = {
        val output =
          try transformer.onComplete
          catch {
            case t: Throwable ⇒
              onError(t)
              return
          }
        deliver(output.toList, complete)
      }

      override def onError(cause: Throwable) = completeWithError(cause)

      // default `whenRemainingOutputIsDone`
      def apply(): Unit = {
        val isComplete =
          try transformer.isComplete
          catch {
            case t: Throwable ⇒
              onError(t)
              upstream.cancel()
              return
          }
        if (isComplete) {
          complete()
          upstream.cancel()
        } else if (requested > 0) {
          become(startBehavior)
          upstream.requestMore(1)
        } else become(startBehavior)
      }
    }

  class OutputPending(remaining: List[Any], var whenRemainingOutputIsDone: () ⇒ Unit) extends Behavior {
    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) deliver(remaining, whenRemainingOutputIsDone)
    }
    override def onComplete() = whenRemainingOutputIsDone = complete
    override def onError(cause: Throwable) = completeWithError(cause)
  }

  @tailrec final def deliver(output: List[Any], whenRemainingOutputIsDone: () ⇒ Unit): Unit =
    if (output.nonEmpty) {
      if (requested > 0) {
        downstream.onNext(output.head) // might re-enter into `requestMore` or `cancel`
        requested -= 1
        deliver(output.tail, whenRemainingOutputIsDone)
      } else become(new OutputPending(output, whenRemainingOutputIsDone))
    } else whenRemainingOutputIsDone()

  def complete(): Unit = {
    downstream.onComplete()
    transformer.cleanup()
  }

  def completeWithError(cause: Throwable): Unit = {
    downstream.onError(cause)
    transformer.cleanup()
  }
}
