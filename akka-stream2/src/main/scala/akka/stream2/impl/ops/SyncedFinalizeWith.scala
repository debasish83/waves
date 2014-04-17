package akka.stream2.impl
package ops

import org.reactivestreams.api.Producer
import akka.stream2.impl.OperationProcessor.SubDownstreamHandling

class SyncedFinalizeWith(finalizer: Producer[Option[Any]])(implicit val upstream: Upstream, val downstream: Downstream,
                                                           ctx: OperationProcessor.Context)
  extends OperationImpl.Stateful {

  val finalizerUpstream = ctx.requestSubUpstream(finalizer, behavior.asInstanceOf[SubDownstreamHandling])
  val startBehavior = behavior

  def initialBehavior = ???
}