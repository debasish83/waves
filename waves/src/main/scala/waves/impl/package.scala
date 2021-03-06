/*
 * Copyright (C) 2014 Mathias Doenitz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package waves

import org.reactivestreams.spi.Subscription
import Operation.~>

package object impl {
  private[waves]type ==>[-I, +O] = Operation[I, O]

  // shorten entry point into operations DSL
  def operation[T]: Operation.Identity[T] = Operation.Identity()

  type Upstream = Subscription // the interface is identical, should we nevertheless use a separate type?

  /**
   * Converts the model Operation into a double-linked chain of OperationImpls
   * whereby the individual OperationImpl instances are separated by StreamConnectors.
   */
  private[impl] def materialize(op: OperationX,
                                upstream: StreamConnector, downstream: StreamConnector,
                                ctx: OperationProcessor.Context): Unit =
    op match {
      case head ~> tail ⇒
        val connector = new StreamConnector
        materialize(head, upstream, connector, ctx)
        materialize(tail, connector, downstream, ctx)
      case _ ⇒
        val opImpl = OperationImpl(op)(upstream, downstream, ctx)
        upstream.connectDownstream(opImpl)
        downstream.connectUpstream(opImpl)
    }
}