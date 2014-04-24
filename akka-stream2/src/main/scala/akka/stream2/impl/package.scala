package akka.stream2

import org.reactivestreams.spi.Subscription

package object impl {
  private[stream2]type ==>[-I, +O] = Operation[I, O]

  // shorten entry point into operations DSL
  def operation[T]: Operation.Identity[T] = Operation.Identity()

  type Upstream = Subscription // the interface is identical, should we nevertheless use a separate type?
}

package impl {
  // same as `Subscriber[T]`, but untyped and without `onSubscribe` and the "must be async" semantics
  trait Downstream {
    def onNext(element: Any): Unit
    def onComplete(): Unit
    def onError(cause: Throwable): Unit
  }
}
