package akka.stream2.impl
package ops

import scala.annotation.tailrec
import OperationImpl.Terminated

class Multiply(factor: Int)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Stateful {

  require(factor > 0)

  var requested = 0
  val startBehavior = behavior

  def initialBehavior: Behavior =
    new Behavior {
      override def requestMore(elements: Int): Unit = {
        requested = elements
        become(waitingForNextElement)
        upstream.requestMore(1)
      }
    }

  val waitingForNextElement =
    new Behavior {
      override def requestMore(elements: Int) = requested += elements
      override def onNext(element: Any) = {
        produce(factor)
        @tailrec def produce(remaining: Int): Unit =
          if (requested > 0) {
            if (remaining > 0) {
              requested -= 1
              downstream.onNext(element)
              produce(remaining - 1)
            } else upstream.requestMore(1)
          } else if (remaining > 0) become(new WaitingForRequestMore(element, remaining))
          else become(startBehavior)
      }
    }

  // when we enter this state `requested` is zero
  class WaitingForRequestMore(element: Any, var remaining: Int) extends Behavior {
    var upstreamCompleted = false
    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) produce()
      @tailrec def produce(): Unit =
        if (remaining > 0) {
          if (requested > 0) {
            downstream.onNext(element)
            requested -= 1
            remaining -= 1
            produce()
          } // else break loop and wait for next requestMore
        } else if (upstreamCompleted) {
          become(Terminated)
          downstream.onComplete()
        } else {
          become(waitingForNextElement)
          upstream.requestMore(1)
        }
    }
    override def onComplete() = upstreamCompleted = true
  }
}