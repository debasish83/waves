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
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.concurrent.ExecutionContext
import org.reactivestreams.api.{ Consumer, Producer }
import Operation._

trait OperationApi[A] extends Any {
  import OperationApi._

  type Res[_]

  def append[B](next: A ==> B): Res[B]

  // appends a simple buffer element which eagerly requests from upstream and
  // dispatches to downstream up to the given max buffer size
  def buffer(size: Int): Res[A] = {
    require(Integer.lowestOneBit(size) == size, "size must be a power of 2")
    append(Buffer(size))
  }

  // appends the given flow to the end of this stream
  def concat(next: ⇒ Producer[A]): Res[A] =
    append(Concat(next _))

  // filters the stream with a partial function and maps to its results
  def collect[B](pf: PartialFunction[A, B]): Res[B] =
    transform {
      new Transformer[A, B] with (B ⇒ Seq[B]) {
        val pfa: PartialFunction[A, Seq[B]] = pf andThen this
        val nil: A ⇒ Seq[B] = _ ⇒ Nil
        def apply(b: B): Seq[B] = b :: Nil
        def onNext(elem: A) = pfa.applyOrElse(elem, nil)
      }
    }

  // completes the stream (and cancels the upstream) after the first element for
  // which the given function returns true
  def completeAfter(f: A ⇒ Boolean): Res[A] =
    transform {
      new Transformer[A, A] {
        private[this] var complete = false
        override def isComplete = complete
        def onNext(elem: A) = {
          complete = f(elem)
          elem :: Nil
        }
      }
    }

  // flattens the upstream by concatenation
  // only available if the stream elements are themselves producable as a Producer[B]
  def concatAll[B](implicit ev: Producable[A, B]): Res[B] =
    append(ConcatAll[A, B])

  // alternative `concatAll` implementation
  def concatAll2[B](implicit ev: Producable[A, B], ec: ExecutionContext): Res[B] =
    outerMap[B] {
      case Producable(FanOut.Tee(p1, p2)) ⇒ Flow(p1).head.concat(Flow(p2).tail.concatAll2.toProducer).toProducer
    }

  // "compresses" a fast upstream by keeping one element buffered and reducing surplus values using the given function
  // consumes at max rate, produces no faster than the upstream
  def compress[B](seed: B)(f: (B, A) ⇒ B): Res[B] =
    append {
      CustomBuffer[A, B, Either[B, B]]( // Left(c) = we need to request from upstream first, Right(c) = we can dispatch to downstream
        seed = Left(seed),
        compress = (either, a) ⇒ Right(f(either match {
          case Left(x)  ⇒ x
          case Right(x) ⇒ x
        }, a)),
        expand = {
          case x @ Left(_) ⇒ (x, None)
          case Right(b)    ⇒ (Left(b), Some(b))
        },
        canConsume = _ ⇒ true)
    }

  // adds (bounded or unbounded) pressure elasticity
  // consumes at max rate as long as `canConsume` is true,
  // produces no faster than the rate with which `expand` produces B values
  def customBuffer[B, S](seed: S)(compress: (S, A) ⇒ S)(expand: S ⇒ (S, Option[B]))(canConsume: S ⇒ Boolean): Res[B] =
    append(CustomBuffer(seed, compress, expand, canConsume))

  // drops the first n upstream values
  // consumes the first n upstream values at max rate, afterwards directly copies upstream
  def drop(n: Int): Res[A] =
    append(Drop(n))

  // produces one boolean for the first T that satisfies p
  // consumes at max rate until p(t) becomes true, unsubscribes afterwards
  def exists(p: A ⇒ Boolean): Res[Boolean] =
    mapFind(x ⇒ if (p(x)) SomeTrue else None, SomeFalse)

  // "expands" a slow upstream by buffering the last upstream element and producing it whenever requested
  // consumes at max rate, produces at max rate once the first upstream value has been buffered
  def expand[S](seed: S)(produce: S ⇒ (S, A)): Res[A] =
    append {
      CustomBuffer[A, A, Option[A]](
        seed = None,
        compress = (_, x) ⇒ Some(x),
        expand = s ⇒ (s, s),
        canConsume = _ ⇒ true)
    }

  // filters a streams according to the given predicate
  // immediately consumes more whenever p(t) is false
  def filter(p: A ⇒ Boolean): Res[A] =
    append(Filter(p))

  // produces the first T that satisfies p
  // consumes at max rate until p(t) becomes true, unsubscribes afterwards
  def find(p: A ⇒ Boolean): Res[A] =
    mapFind(x ⇒ if (p(x)) Some(x) else None, None)

  // classic fold
  // consumes at max rate, produces only one value
  def fold[B](seed: B)(f: (B, A) ⇒ B): Res[B] =
    append(Fold(seed, f))

  // produces one boolean (if all upstream values satisfy p emits true otherwise false)
  // consumes at max rate until p(t) becomes false, unsubscribes afterwards
  def forAll(p: A ⇒ Boolean): Res[Boolean] =
    mapFind(x ⇒ if (!p(x)) SomeFalse else None, SomeTrue)

  // "extracts" the first element
  // only available if the stream elements are themselves producable as a Producer[B]
  def head[B](implicit ev: Producable[A, B]): Res[B] =
    append(Head[A, B])

  // maps the inner streams into a (head, tail) Tuple each
  // only available if the stream elements are themselves producable as a Producer[B]
  def headAndTail[B](implicit ev: Producable[A, B], ec: ExecutionContext): Res[(B, Producer[B])] = {
    def headTail: A ⇒ Producer[(B, Producer[B])] = {
      case Producable(FanOut.Tee(p1, p2)) ⇒
        val tailStream = Flow(p2).tail.toProducer
        Flow(p1).headStream.map(_ -> tailStream).toProducer
    }
    append(Map(headTail) ~> ConcatAll[Producer[(B, Producer[B])], (B, Producer[B])])
  }

  // produces the first upstream element, unsubscribes afterwards
  def headStream: Res[A] =
    append(Take(1))

  // maps the given function over the upstream
  def map[B](f: A ⇒ B): Res[B] =
    append(Map(f))

  // combined map & concat operation
  def mapConcat[B, P](f: A ⇒ P)(implicit ev: Producable[P, B]): Res[B] =
    append(Map[A, Producer[B]](a ⇒ ev(f(a))) ~> ConcatAll[Producer[B], B])

  // produces the first A returned by f or optionally the given default value
  // consumes at max rate until f returns a Some, unsubscribes afterwards
  def mapFind[B](f: A ⇒ Option[B], default: ⇒ Option[B]): Res[B] =
    transform {
      new Transformer[A, B] {
        def onNext(elem: A) = f(elem).toList
        override def onComplete = default.toList
      }
    }

  // merges the values produced by the given stream into the consumed stream
  def merge[AA >: A](secondary: Producer[_ <: AA]): Res[AA] =
    append(Merge[A, AA](secondary))

  // merges the values produced by the given stream into the consumed stream
  def mergeToEither[L](left: Producer[L]): Res[Either[L, A]] =
    append(MergeToEither[L, A](left))

  // repeats each element coming in from upstream `factor` times
  def multiply(factor: Int): Res[A] =
    append(Multiply[A](factor))

  // attaches the given callback which "listens" to `cancel' events without otherwise affecting the stream
  def onCancel[U](callback: ⇒ U): Res[A] =
    onEventPF { case StreamEvent.Cancel ⇒ callback }

  // attaches the given callback which "listens" to `onComplete' events without otherwise affecting the stream
  def onComplete[U](callback: ⇒ U): Res[A] =
    onEventPF { case StreamEvent.OnComplete ⇒ callback }

  // attaches the given callback which "listens" to `onNext' events without otherwise affecting the stream
  def onElement(callback: A ⇒ Unit): Res[A] =
    onEventPF { case StreamEvent.OnNext(element) ⇒ callback(element) }

  // attaches the given callback which "listens" to `onError' events without otherwise affecting the stream
  def onError[U](callback: Throwable ⇒ U): Res[A] =
    onEventPF { case StreamEvent.OnError(cause) ⇒ callback(cause) }

  // attaches the given callback which "listens" to all stream events without otherwise affecting the stream
  def onEvent(callback: StreamEvent[A] ⇒ Unit): Res[A] =
    append(OnEvent(callback))

  // attaches the given callback which "listens" to all stream events without otherwise affecting the stream
  def onEventPF(callback: PartialFunction[StreamEvent[A], Unit]): Res[A] =
    onEvent(ev ⇒ callback.applyOrElse(ev, (_: Any) ⇒ ()))

  // attaches the given callback which "listens" to `requestMore' events without otherwise affecting the stream
  def onRequestMore(callback: Int ⇒ Unit): Res[A] =
    onEventPF { case StreamEvent.RequestMore(elements) ⇒ callback(elements) }

  // attaches the given callback which "listens" to `onComplete' and `onError` events without otherwise affecting the stream
  def onTerminate(callback: Option[Throwable] ⇒ Unit): Res[A] =
    onEventPF {
      case StreamEvent.OnComplete     ⇒ callback(None)
      case StreamEvent.OnError(cause) ⇒ callback(Some(cause))
    }

  // chains in the given operation
  def op[B](operation: A ==> B): Res[B] =
    append(operation)

  // transforms the underlying stream itself (not its elements) with the given function
  def outerMap[B](f: Producer[A] ⇒ Producer[B]): Res[B] =
    append(OuterMap(f))

  // debugging help: simply printlns all events passing through
  def printEvent(marker: String): Res[A] =
    onEvent(ev ⇒ println(s"$marker: $ev"))

  // lifts errors from upstream back into the main data flow before completing normally
  def recover[B <: A, P](pf: PartialFunction[Throwable, P])(implicit ev: Producable[P, B]): Res[B] =
    append(Recover(pf andThen ev))

  // general stream transformation
  def transform[B](transformer: Transformer[A, B]): Res[B] =
    append(Transform(transformer))

  // lifts regular data and errors from upstream into a Try
  def tryRecover: Res[Try[A]] =
    append {
      Map[A, Try[A]](Success(_)) ~> Recover[Try[A], Try[A]] {
        case NonFatal(e) ⇒ StreamProducer(Failure(e) :: Nil)
      }
    }

  // splits the upstream into sub-streams based on the commands produced by the given function,
  // never produces empty sub-streams
  def split(f: A ⇒ Split.Command): Res[Producer[A]] =
    append(Split(f))

  // drops the first upstream value and forwards the remaining upstream
  // consumes the first upstream value immediately, afterwards directly copies upstream
  def tail: Res[A] =
    append(Drop(1))

  // forwards the first n upstream values, unsubscribes afterwards
  def take(n: Int): Res[A] =
    append(Take[A](n))

  // splits the upstream into two downstreams that will receive the exact same elements in the same sequence
  def tee(consumer: Consumer[A]): Res[A] =
    tee(_.produceTo(consumer))

  // splits the upstream into two downstreams that will receive the exact same elements in the same sequence
  def tee(secondary: Producer[A] ⇒ Unit): Res[A] =
    append(Tee[A](secondary))

  // forwards as long as p returns true, unsubscribes afterwards
  def takeWhile(p: A ⇒ Boolean): Res[A] =
    transform {
      new Transformer[A, A] {
        private[this] var _isComplete = false
        override def isComplete = _isComplete
        def onNext(elem: A) = if (p(elem)) elem :: Nil else { _isComplete = true; Nil }
      }
    }

  // splits an upstream of tuples into two downstreams which each receive a tuple component
  def unzip[T1, T2](secondary: Producer[T2] ⇒ Unit)(implicit ev: A =:= (T1, T2)): Res[T1] =
    append(Unzip[T1, T2](secondary).asInstanceOf[A ==> T1])

  // combines the upstream and the given flow into tuples
  def zip[B](secondary: Producer[B]): Res[(A, B)] =
    append(Zip[A, B](secondary))
}

object OperationApi {
  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}