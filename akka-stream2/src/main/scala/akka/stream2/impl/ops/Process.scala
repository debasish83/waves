package akka.stream2.impl
package ops

import scala.annotation.tailrec
import akka.stream2.impl.OperationImpl.Terminated
import akka.stream2.Operation.Process._

class Process(seed: Any,
              fOnNext: (Any, Any) ⇒ Command[Any, Any],
              fOnComplete: Any ⇒ Command[Any, Any])(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Stateful {

  var state = seed
  var requested = 0
  var upstreamCompleted = false
  var error: Option[Throwable] = None

  val initialBehavior = new Behavior {
    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) upstream.requestMore(1)
    }
    override def onNext(element: Any): Unit = {
      val cmd =
        try fOnNext(state, element)
        catch {
          case t: Throwable ⇒
            become(Terminated)
            downstream.onError(t)
            upstream.cancel()
            return
        }
      process(cmd)
    }
    override def onComplete() = {
      upstreamCompleted = true
      complete()
    }
    override def onError(cause: Throwable) = {
      error = Some(cause)
      complete()
    }
  }

  def draining(cmd: Command[Any, Any]): Behavior =
    new Behavior {
      override def requestMore(elements: Int) = {
        requested += elements
        if (requested == elements) process(cmd)
      }
      override def onComplete() = upstreamCompleted = true
      override def onError(cause: Throwable) = error = Some(cause)
    }

  @tailrec private def process(cmd: Command[Any, Any]): Unit =
    cmd match {
      case Emit(value, andThen) ⇒
        if (requested > 0) {
          downstream.onNext(value) // might re-enter into `requestMore` or `cancel` of the current behavior
          requested -= 1
          process(andThen)
        } else become(draining(cmd))

      case Continue(nextState) ⇒
        state = nextState
        if (upstreamCompleted || error.isDefined) {
          complete()
        } else if (requested > 0) {
          become(initialBehavior)
          upstream.requestMore(1)
        } else become(initialBehavior)

      case Stop ⇒
        become(Terminated)
        if (upstreamCompleted) {
          downstream.onComplete()
        } else if (error.isDefined) {
          downstream.onError(error.get)
        } else {
          downstream.onComplete()
          upstream.cancel()
        }
    }

  def complete(): Unit = {
    val cmd =
      try fOnComplete(state)
      catch {
        case t: Throwable ⇒
          become(Terminated)
          downstream.onError(t)
          upstream.cancel()
          return
      }
    process(cmd)
  }
}
