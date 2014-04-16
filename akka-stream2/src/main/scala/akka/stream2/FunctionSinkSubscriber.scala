package akka.stream2

import scala.concurrent.ExecutionContext
import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{ Subscription, Subscriber }

class FunctionSinkSubscriber[T](f: T ⇒ Unit)(implicit executor: ExecutionContext) extends Consumer[T] with Subscriber[T] {
  def getSubscriber: Subscriber[T] = this

  @volatile private var subscription: Option[Subscription] = None

  def onSubscribe(subscription: Subscription): Unit = {
    this.subscription = Some(subscription)
    executor.execute {
      new Runnable {
        def run() = subscription.requestMore(1)
      }
    }
  }

  def onNext(element: T): Unit =
    executor.execute {
      new Runnable {
        def run() = subscription match {
          case Some(sub) ⇒
            sub.requestMore(1)
            f(element) // TODO: what about exceptions thrown from f?
          case _ ⇒ throw new IllegalStateException
        }
      }
    }

  def onComplete(): Unit = ()

  def onError(cause: Throwable): Unit = ()
}
