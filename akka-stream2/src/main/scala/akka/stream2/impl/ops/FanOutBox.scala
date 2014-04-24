package akka.stream2.impl
package ops

import org.reactivestreams.api.Producer
import OperationProcessor.SubUpstreamHandling
import akka.stream2.FanOut

class FanOutBox(fanOutProvider: FanOut.Provider[FanOut],
                secondary: Producer[Any] ⇒ Unit)(implicit val upstream: Upstream,
                                                 val downstream: Downstream, ctx: OperationProcessor.Context)
  extends OperationImpl.Abstract with SubUpstreamHandling {

  val secondaryDownstream = ctx.requestSubDownstream(this)
  secondary(secondaryDownstream)

  val fanOut = fanOutProvider(upstream, downstream, secondaryDownstream)

  override def onNext(element: Any): Unit = fanOut.onNext(element)
  override def onComplete(): Unit = fanOut.onComplete()
  override def onError(cause: Throwable): Unit = fanOut.onError(cause)

  override def requestMore(elements: Int): Unit = fanOut.primaryRequestMore(elements)
  override def cancel(): Unit = fanOut.primaryCancel()

  def subRequestMore(elements: Int): Unit = fanOut.secondaryRequestMore(elements)
  def subCancel(): Unit = fanOut.secondaryCancel()
}