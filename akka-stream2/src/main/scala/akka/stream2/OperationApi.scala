package akka.stream2

import scala.language.{ higherKinds, implicitConversions }
import scala.collection.immutable
import org.reactivestreams.api.{ Consumer, Producer }
import Operation._

trait OperationApi1[A] extends Any {
  import OperationApi1._

  type Res[_]

  def ~>[B](next: A ==> B): Res[B]

  // appends the given flow to the end of this stream
  def concat(producer: Producer[A]): Res[A] =
    this ~> Concat(producer)

  // adds (bounded or unbounded) pressure elasticity
  // consumes at max rate as long as `canConsume` is true,
  // produces no faster than the rate with which `expand` produces B values
  def buffer[B, S](seed: S)(compress: (S, A) ⇒ S)(expand: S ⇒ (S, Option[B]))(canConsume: S ⇒ Boolean): Res[B] =
    this ~> Buffer(seed, compress, expand, canConsume)

  // filters the stream with the partial function and maps to its results
  def collect[B](pf: PartialFunction[A, B]): Res[B] =
    transform {
      new Transformer[A, B] with (B ⇒ immutable.Seq[B]) {
        val pfa: PartialFunction[A, immutable.Seq[B]] = pf andThen this
        val nil: A ⇒ immutable.Seq[B] = _ ⇒ Nil
        def apply(b: B): immutable.Seq[B] = b :: Nil
        def onNext(elem: A) = pfa.applyOrElse(elem, nil)
      }
    }

  // "compresses" a fast upstream by keeping one element buffered and reducing surplus values using the given function
  // consumes at max rate, produces no faster than the upstream
  def compress[B](seed: B)(f: (B, A) ⇒ B): Res[B] =
    this ~> Buffer[A, B, Either[B, B]]( // Left(c) = we need to request from upstream first, Right(c) = we can dispatch to downstream
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

  // drops the first n upstream values
  // consumes the first n upstream values at max rate, afterwards directly copies upstream
  def drop(n: Int): Res[A] = this ~> Drop(n)

  // produces one boolean for the first T that satisfies p
  // consumes at max rate until p(t) becomes true, unsubscribes afterwards
  def exists(p: A ⇒ Boolean): Res[Boolean] =
    mapFind(x ⇒ if (p(x)) SomeTrue else None, SomeFalse)

  // "expands" a slow upstream by buffering the last upstream element and producing it whenever requested
  // consumes at max rate, produces at max rate once the first upstream value has been buffered
  def expand[S](seed: S)(produce: S ⇒ (S, A)): Res[A] =
    this ~> Buffer[A, A, Option[A]](
      seed = None,
      compress = (_, x) ⇒ Some(x),
      expand = s ⇒ (s, s),
      canConsume = _ ⇒ true)

  // filters a streams according to the given predicate
  // immediately consumes more whenever p(t) is false
  def filter(p: A ⇒ Boolean): Res[A] = this ~> Filter(p)

  // produces the first T that satisfies p
  // consumes at max rate until p(t) becomes true, unsubscribes afterwards
  def find(p: A ⇒ Boolean): Res[A] =
    mapFind(x ⇒ if (p(x)) Some(x) else None, None)

  // classic fold
  // consumes at max rate, produces only one value
  def fold[B](seed: B)(f: (B, A) ⇒ B): Res[B] = this ~> Fold(seed, f)

  // produces one boolean (if all upstream values satisfy p emits true otherwise false)
  // consumes at max rate until p(t) becomes false, unsubscribes afterwards
  def forAll(p: A ⇒ Boolean): Res[Boolean] =
    mapFind(x ⇒ if (!p(x)) SomeFalse else None, SomeTrue)

  // produces the first upstream element, unsubscribes afterwards
  def head: Res[A] = this ~> Take(1)

  // maps the given function over the upstream
  // does not affect consumption or production rates
  def map[B](f: A ⇒ B): Res[B] = this ~> Map(f)

  // combined map & concat operation
  // consumes no faster than the downstream, produces no faster than upstream or generated flows
  def mapConcat[B, CC](f: A ⇒ CC)(implicit ev: CC <:< Producer[B]): Res[B] =
    this ~> (Map[A, Producer[B]](b ⇒ ev(f(b))) ~> Flatten[B]())

  // produces the first A returned by f or optionally the given default value
  // consumes at max rate until f returns a Some, unsubscribes afterwards
  def mapFind[B](f: A ⇒ Option[B], default: ⇒ Option[B]): Res[B] =
    transform {
      new Transformer[A, B] {
        def onNext(elem: A) = f(elem).toList
        override def onComplete = default.toList
      }
    }

  // merges the values produced by the given flow into the consumed stream
  // consumes from the upstream and the given flow no faster than the downstream
  // produces no faster than the combined rate from upstream and the given flow
  def merge[BB >: A](producer: Producer[BB]): Res[BB] = this ~> Merge(producer)

  // repeats each element coming in from upstream `factor` times
  // consumes from the upstream and downstream consumption rate divided by factor
  // produces the `factor` elements for one element from upstream at max rate
  def multiply(factor: Int): Res[A] = this ~> Multiply[A](factor)

  // attaches the given callback which "listens" to `onComplete' events
  // without otherwise affecting the stream
  def onComplete[U](callback: ⇒ U): Res[A] =
    onTerminate { errorOption ⇒ if (errorOption.isEmpty) callback }

  // attaches the given callback which "listens" to `onError' events
  // without otherwise affecting the stream
  def onError[U](callback: Throwable ⇒ U): Res[A] =
    onTerminate { errorOption ⇒ if (errorOption.isDefined) callback(errorOption.get) }

  // attaches the given callback which "listens" to `onComplete' and `onError` events
  // without otherwise affecting the stream
  def onTerminate[U](callback: Option[Throwable] ⇒ U): Res[A] =
    this ~> OnTerminate[A](callback)

  // chains in the given operation
  def op[B](operation: A ==> B): Res[B] = this ~> operation

  // general stream transformation
  def transform[B](transformer: Transformer[A, B]): Res[B] =
    this ~> Transform(transformer)

  // splits the upstream into sub-streams based on the commands produced by the given function,
  // never produces empty sub-streams
  def split(f: A ⇒ Split.Command): Res[Producer[A]] = this ~> Split(f)

  // drops the first upstream value and forwards the remaining upstream
  // consumes the first upstream value immediately, afterwards directly copies upstream
  def tail: Res[A] = this ~> Drop(1)

  // forwards the first n upstream values, unsubscribes afterwards
  // consumes no faster than the downstream, produces no faster than the upstream
  def take(n: Int): Res[A] = this ~> Take[A](n)

  // splits the upstream into two downstreams that will receive the exact same elements in the same sequence
  def tee(consumer: Consumer[A]): Res[A] = tee(_.produceTo(consumer))

  // splits the upstream into two downstreams that will receive the exact same elements in the same sequence
  def tee(f: Producer[A] ⇒ Unit): Res[A] = this ~> Tee[A](f)

  // forwards as long as p returns true, unsubscribes afterwards
  // consumes no faster than the downstream, produces no faster than the upstream
  def takeWhile(p: A ⇒ Boolean): Res[A] =
    transform {
      new Transformer[A, A] {
        private[this] var _isComplete = false
        override def isComplete = _isComplete
        def onNext(elem: A) = if (p(elem)) elem :: Nil else { _isComplete = true; Nil }
      }
    }

  // combines the upstream and the given flow into tuples
  // produces at the rate of the slower upstream (i.e. no values are dropped)
  // consumes from the upstream no faster than the downstream consumption rate or the production rate of the given flow
  // consumes from the given flow no faster than the downstream consumption rate or the upstream production rate
  def zip[B](producer: Producer[B]): Res[(A, B)] = this ~> Zip(producer)
}

object OperationApi1 {
  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}

trait OperationApi2[A] extends Any {
  type Res[_]

  def ~>[B](next: Producer[A] ==> B): Res[B]

  // flattens the upstream by concatenation
  // consumes no faster than the downstream, produces no faster than the flows in the upstream
  def flatten: Res[A] = this ~> Flatten()

  // splits nested streams into a tuple of head-element and tail stream
  def headAndTail: Res[(A, Producer[A])] = this ~> HeadAndTail()
}
