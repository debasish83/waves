package akka.stream2
package impl
package ops

import org.reactivestreams.api.Producer
import OperationProcessor.SubUpstreamHandling

class FanOutBox(fanOut: FanOut.Provider[FanOut],
                secondary: Producer[Any] ⇒ Unit)(implicit val upstream: Upstream,
                                                 val downstream: Downstream, ctx: OperationProcessor.Context)
  extends OperationImpl.Abstract with SubUpstreamHandling {

  val secondaryDownstream = ctx.requestSubDownstream(this)
  secondary(secondaryDownstream)

  val f = fanOut(upstream, downstream, secondaryDownstream)

  override def onNext(element: Any): Unit = f.onNext(element)
  override def onComplete(): Unit = f.onComplete()
  override def onError(cause: Throwable): Unit = f.onError(cause)

  override def requestMore(elements: Int): Unit = f.primaryRequestMore(elements)
  override def cancel(): Unit = f.primaryCancel()

  def subRequestMore(elements: Int): Unit = f.secondaryRequestMore(elements)
  def subCancel(): Unit = f.secondaryCancel()
}