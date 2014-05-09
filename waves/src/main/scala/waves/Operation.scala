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

import scala.language.{ higherKinds, implicitConversions }
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.ExecutionContext
import org.reactivestreams.api.{ Producer, Consumer, Processor }
import waves.impl.OperationProcessor

sealed trait OperationX // untyped base trait used for dealing with untyped operations

sealed abstract class Operation[-A, +B] extends OperationX {
  import Operation._

  type Res[C] = Operation[A @uncheckedVariance, C]

  def ~>[C](other: B ==> C): Res[C] =
    (this, other) match {
      case (_: Identity[_], _) ⇒ other.asInstanceOf[A ==> C]
      case (_, _: Identity[_]) ⇒ this.asInstanceOf[A ==> C]
      case _                   ⇒ Operation.~>(this, other)
    }
}

object Operation {

  def apply[A]: Identity[A] = Identity[A]()

  implicit class Api[A, B](val op: A ==> B) extends OperationApi[B, Operation[A, B]#Res] {
    def append[C](next: B ==> C): Operation[A, B]#Res[C] = op ~> next

    def res2Api[T](op: Operation[A, B]#Res[T]) = Api(op)

    def toProcessor(implicit ec: ExecutionContext): Processor[A, B] =
      new OperationProcessor(op) // TODO: introduce implicit settings allowing for buffer size config

    def produceTo(consumer: Consumer[B])(implicit ec: ExecutionContext): Consumer[A] = {
      val processor = toProcessor
      processor.produceTo(consumer)
      processor
    }

    def drain(callback: B ⇒ Unit)(implicit ec: ExecutionContext): Consumer[A] =
      onElement(callback) produceTo StreamConsumer.blackHole[B]
  }

  /////////////////////////// MODEL ////////////////////////////

  final case class ~>[A, B, C](f: A ==> B, g: B ==> C) extends (A ==> C)

  final case class Buffer[T](size: Int) extends (T ==> T) {
    require(Integer.lowestOneBit(size) == size && size >= 2, "size must be a power of 2 and >= 2")
  }

  final case class CustomBuffer[A, B, S](seed: S,
                                         compress: (S, A) ⇒ S,
                                         expand: S ⇒ (S, Option[B]),
                                         canConsume: S ⇒ Boolean) extends (A ==> B)

  final case class Concat[A, AA >: A](next: () ⇒ Producer[_ <: AA]) extends (A ==> AA)

  final case class ConcatAll[A, B](implicit ev: Producable[A, B]) extends (A ==> B)

  final case class Drop[T](n: Int) extends (T ==> T)

  final case class Filter[T](p: T ⇒ Boolean) extends (T ==> T)

  final case class Fold[A, B](seed: B, f: (B, A) ⇒ B) extends (A ==> B)

  final case class Head[A, B](implicit ev: Producable[A, B]) extends (A ==> B)

  sealed abstract class Identity[A] extends (A ==> A)
  object Identity extends Identity[Nothing] {
    private[this] final val unapplied = Some(this)
    def apply[T](): Identity[T] = this.asInstanceOf[Identity[T]]
    def unapply[I, O](operation: I ==> O): Option[Identity[I]] =
      if (operation eq this) unapplied.asInstanceOf[Option[Identity[I]]] else None
  }

  final case class Map[A, B](f: A ⇒ B) extends (A ==> B)

  final case class Merge[A, AA >: A](secondary: Producer[_ <: AA]) extends (A ==> AA)

  final case class Multiply[T](factor: Int) extends (T ==> T)

  final case class OnEvent[T](callback: StreamEvent[T] ⇒ Unit) extends (T ==> T)
  sealed trait StreamEvent[+T]
  object StreamEvent {
    case class RequestMore(elements: Int) extends StreamEvent[Nothing]
    case object Cancel extends StreamEvent[Nothing]
    case class OnNext[T](value: T) extends StreamEvent[T]
    case object OnComplete extends StreamEvent[Nothing]
    case class OnError(cause: Throwable) extends StreamEvent[Nothing]
  }

  final case class OuterMap[A, B](f: Producer[A] ⇒ Producer[B]) extends (A ==> B)

  final case class Recover[A, AA >: A](pf: PartialFunction[Throwable, Producer[AA]]) extends (A ==> AA)

  final case class Split[T](f: T ⇒ Split.Command) extends (T ==> Producer[T])
  object Split {
    sealed trait Command
    case object Drop extends Command // drop the current element
    case object Append extends Command // append to current sub-stream, if no sub-stream is currently open start a new one
    case object Last extends Command // append element (same as `Append`) and complete the current sub-stream afterwards
    case object First extends Command // complete the current sub-stream (if there is one) and start a new one with the current element
  }

  final case class Take[T](n: Int) extends (T ==> T) {
    require(n > 0)
  }

  final case class Tee[T](secondary: Producer[T] ⇒ Unit) extends (T ==> T)

  final case class Transform[A, B](transformer: Transformer[A, B]) extends (A ==> B)

  /**
   * The driving logic is this:
   * 1. Demand from downstream is directly propagated to upstream.
   * 2. When an element comes in from upstream `onNext` is called and its results dispatched to downstream.
   * 3. When all elements have been dispatched and the upstream is still uncompleted `isComplete` is being called.
   * 4. If `isComplete` is true the downstream is completed, the upstream is cancelled and `cleanup` called.
   * 5. When the upstream is completed `onComplete` is called and its result dispatched to downstream before
   *    the downstream is completed and `cleanup` called.
   * 6. All errors (from upstream or exceptions thrown by any method) are immediately dispatched to downstream,
   *    the upstream is cancelled (if the error is an exception thrown by a method) and `cleanup` is called.
   */
  trait Transformer[-A, +B] {
    def onNext(elem: A): Seq[B]
    def isComplete: Boolean = false
    def onComplete: Seq[B] = Nil
    def cleanup(): Unit = ()
  }

  final case class Unzip[A, B](secondary: Producer[B] ⇒ Unit) extends ((A, B) ==> A)

  final case class Zip[A, B](secondary: Producer[B]) extends (A ==> (A, B))
}