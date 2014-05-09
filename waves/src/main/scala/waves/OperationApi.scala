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

import java.util.NoSuchElementException
import scala.language.{ higherKinds, implicitConversions }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.concurrent.ExecutionContext
import scala.collection.immutable
import org.reactivestreams.api.Producer
import Operation._

trait OperationApi[A, Res[_]] extends Any {
  import OperationApi._

  def append[B](next: A ==> B): Res[B]

  implicit def res2Api[T](res: Res[T]): OperationApi[T, Res]

  // appends a simple buffer element which eagerly requests from upstream and
  // dispatches to downstream up to the given max buffer size
  def buffer(size: Int): Res[A] =
    append(Buffer(size))

  // appends the given flow to the end of this stream
  def concat[AA >: A](next: ⇒ Producer[_ <: AA]): Res[AA] =
    append(Concat[A, AA](next _))

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

  // flattens the upstream by concatenation
  // only available if the stream elements are themselves producable as a Producer[B]
  def concatAll[B](implicit ev: Producable[A, B]): Res[B] =
    append(ConcatAll[A, B])

  // alternative `concatAll` implementation
  def concatAll2[B](implicit ev: Producable[A, B], ec: ExecutionContext): Res[B] =
    outerMap[B] {
      case Producable(FanOut.Tee(p1, p2)) ⇒
        val tail = Flow(p2).tail.toProducer
        Flow(p1).head.concat(Flow(tail).concatAll2.toProducer).toProducer
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

  // drop the last n upstream values
  // NOTE: this operation has to buffer n elements and will only start producing the first
  // element after n + 1 elements have been received from upstream
  def dropLast(n: Int): Res[A] = ??? // TODO: ringbuffer-based implementation

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

  // groups the upstream elements into immutable seqs containing not more than the given number of items
  def groupedIntoSeqs(n: Int): Res[immutable.Seq[A]] = {
    require(n > 0, "n must be > 0")
    transform {
      new Transformer[A, immutable.Seq[A]] {
        val builder = new immutable.VectorBuilder[A]
        var count = 0
        builder.sizeHint(n)
        def onNext(elem: A) = {
          builder += elem
          count += 1
          if (count == n) {
            val group = builder.result()
            builder.clear()
            count = 0
            group :: Nil
          } else Nil
        }
        override def onComplete = if (count == 0) Nil else builder.result() :: Nil
      }
    }
  }

  // groups the upstream into sub-streams containing not more than the given number of items
  def grouped(n: Int): Res[Producer[A]] = {
    require(n > 0, "n must be > 0")
    var count = 0
    split { _ ⇒
      count += 1
      if (count == n) {
        count = 0
        Split.Last
      } else Split.Append
    }
  }

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
        Flow(p1).take(1).map(_ -> tailStream).toProducer
    }
    map(headTail).concatAll
  }

  // maps the upstream onto itself
  def identity: Res[A] =
    append(Identity())

  // maps the given function over the upstream
  def map[B](f: A ⇒ B): Res[B] =
    append(Map(f))

  // combined map & concat operation
  def mapConcat[B, P](f: A ⇒ P)(implicit ev: Producable[P, B]): Res[B] =
    map(a ⇒ ev(f(a))).concatAll

  // produces the first A returned by f or optionally the given default value
  // consumes at max rate until f returns a Some, unsubscribes afterwards
  def mapFind[B](f: A ⇒ Option[B], default: ⇒ Option[B]): Res[B] =
    transform {
      new Transformer[A, B] {
        var _isComplete = false
        override def isComplete = _isComplete
        def onNext(elem: A) = {
          val result = f(elem).toList
          if (result.nonEmpty) _isComplete = true
          result
        }
        override def onComplete = default.toList
      }
    }

  // merges the values produced by the given stream into the consumed stream
  def merge[AA >: A](secondary: Producer[_ <: AA]): Res[AA] =
    append(Merge[A, AA](secondary))

  // merges the values produced by the given stream into the consumed stream
  def mergeToEither[L](left: Producer[L])(implicit ec: ExecutionContext): Res[Either[L, A]] =
    map(Right(_)).merge(Flow(left).map(Left(_)).toProducer)

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

  // partition the upstream in two downstreams based on the given function
  def partition[L, R](secondary: Producer[L] ⇒ Unit)(f: A ⇒ Either[L, R])(implicit ec: ExecutionContext): Res[R] =
    map(f).tee(p ⇒ secondary(Flow(p).collect { case Left(x) ⇒ x }.toProducer)).collect { case Right(x) ⇒ x }

  // debugging help: simply printlns all events passing through
  def printEvent(marker: String): Res[A] =
    onEvent(ev ⇒ println(s"$marker: $ev"))

  // lifts errors from upstream back into the main data flow before completing normally
  def recover[AA >: A, P](pf: PartialFunction[Throwable, P])(implicit ev: Producable[P, AA]): Res[AA] =
    append(Recover(pf andThen ev))

  // reduces the upstream to a single element using the given function
  def reduce[AA >: A](op: (AA, AA) ⇒ AA): Res[AA] =
    transform {
      new Transformer[A, AA] {
        var acc: AA = NoValue.asInstanceOf[AA]
        def onNext(elem: A) = {
          acc = if (NoValue == acc) elem else op(acc, elem)
          Nil
        }
        override def onComplete =
          if (NoValue == acc) throw new NoSuchElementException else acc :: Nil
      }
    }

  // general stream transformation
  def transform[B](transformer: Transformer[A, B]): Res[B] =
    append(Transform(transformer))

  // lifts regular data and errors from upstream into a Try
  def tryRecover: Res[Try[A]] =
    map[Try[A]](Success(_)).recover { case NonFatal(e) ⇒ List(Failure(e)) }

  // splits the upstream into sub-streams based on the commands produced by the given function,
  // never produces empty sub-streams
  def split(f: A ⇒ Split.Command): Res[Producer[A]] =
    append(Split(f))

  // drops the first upstream value and forwards the remaining upstream
  // consumes the first upstream value immediately, afterwards directly copies upstream
  def tail: Res[A] =
    drop(1)

  // forwards the first n upstream values, unsubscribes afterwards
  def take(n: Int): Res[A] =
    append(Take[A](n))

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
  def unzip[T1, T2](secondary: Producer[T2] ⇒ Unit)(implicit ev: A <:< (T1, T2)): Res[T1] =
    append(Unzip[T1, T2](secondary).asInstanceOf[A ==> T1])

  // combines the upstream and the given flow into tuples
  def zip[B](secondary: Producer[B]): Res[(A, B)] =
    append(Zip[A, B](secondary))
}

object OperationApi {
  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}