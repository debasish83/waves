package akka.stream2

import scala.language.{ higherKinds, implicitConversions }
import Operation._

trait OperationApi1[A] extends Any {
  import OperationApi1._

  type Res[_]

  def ~>[B](next: A ==> B): Res[B]

  // appends the given source to the end of this stream
  def append(source: Source[A]): Res[A] =
    this ~> Append(source)

  // adds (bounded or unbounded) pressure elasticity
  // consumes at max rate as long as `canConsume` is true,
  // produces no faster than the rate with which `expand` produces B values
  def buffer[B, S](seed: S)(compress: (S, A) ⇒ S)(expand: S ⇒ (S, Option[B]))(canConsume: S ⇒ Boolean): Res[B] =
    this ~> Buffer(seed, compress, expand, canConsume)

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

  // general flatmap operation
  // consumes no faster than the downstream, produces no faster than upstream or generated sources
  def flatMap[B, CC](f: A ⇒ CC)(implicit ev: CC <:< Source[B]): Res[B] =
    this ~> (Map[A, Source[B]](b ⇒ ev(f(b))) ~> Flatten[B]())

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

  // produces the first A returned by f or optionally the given default value
  // consumes at max rate until f returns a Some, unsubscribes afterwards
  def mapFind[B](f: A ⇒ Option[B], default: ⇒ Option[B]): Res[B] =
    this ~> Process[A, B, Unit](
      seed = (),
      onNext = (_, x) ⇒ f(x) match {
        case Some(value) ⇒ Process.Emit(value, Process.Stop)
        case None        ⇒ ContinueUnit
      },
      onComplete = _ ⇒ default match {
        case Some(value) ⇒ Process.Emit(value, Process.Stop)
        case None        ⇒ Process.Stop
      })

  // merges the values produced by the given source into the consumed stream
  // consumes from the upstream and the given source no faster than the downstream
  // produces no faster than the combined rate from upstream and the given source
  def merge[BB >: A](source: Source[BB]): Res[BB] = this ~> Merge(source)

  // repeats each element coming in from upstream `factor` times
  // consumes from the upstream and downstream consumption rate divided by factor
  // produces the `factor` elements for one element from upstream at max rate
  def multiply(factor: Int): Res[A] = this ~> Multiply[A](factor)

  // chains in the given operation
  def op[B](operation: A ==> B): Res[B] = this ~> operation

  // generalized process potentially producing several output values
  // consumes at max rate as long as `onNext` returns `Continue`
  // produces no faster than the upstream
  def process[S, B](seed: S)(f: (S, A) ⇒ Process.Command[B, S])(onComplete: S ⇒ Process.Command[B, S]): Res[B] =
    this ~> Process(seed, f, onComplete)

  // splits the upstream into sub-streams based on the commands produced by the given function,
  // never produces empty sub-streams
  def split(f: A ⇒ Split.Command): Res[Source[A]] = this ~> Split(f)

  // drops the first upstream value and forwards the remaining upstream
  // consumes the first upstream value immediately, afterwards directly copies upstream
  def tail: Res[A] = this ~> Drop(1)

  // forwards the first n upstream values, unsubscribes afterwards
  // consumes no faster than the downstream, produces no faster than the upstream
  def take(n: Int): Res[A] = this ~> Take[A](n)

  // forwards as long as p returns true, unsubscribes afterwards
  // consumes no faster than the downstream, produces no faster than the upstream
  def takeWhile(p: A ⇒ Boolean): Res[A] =
    this ~> Process[A, A, Unit](
      seed = (),
      onNext = (_, elem) ⇒ if (p(elem)) Process.Emit(elem, ContinueUnit) else Process.Stop,
      onComplete = _ ⇒ Process.Stop)

  // combines the upstream and the given source into tuples
  // produces at the rate of the slower upstream (i.e. no values are dropped)
  // consumes from the upstream no faster than the downstream consumption rate or the production rate of the given source
  // consumes from the given source no faster than the downstream consumption rate or the upstream production rate
  def zip[B](source: Source[B]): Res[(A, B)] = this ~> Zip(source)
}

object OperationApi1 {
  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
  private val _unit = Process.Continue[Nothing, Unit](())
  private def ContinueUnit[T]: Process.Continue[T, Unit] = _unit.asInstanceOf[Process.Continue[T, Unit]]
}

trait OperationApi2[A] extends Any {
  type Res[_]

  def ~>[B](next: Source[A] ==> B): Res[B]

  // flattens the upstream by concatenation
  // consumes no faster than the downstream, produces no faster than the sources in the upstream
  def flatten: Res[A] = this ~> Flatten()

  // splits nested streams into a tuple of head-element and tail stream
  def headAndTail: Res[(A, Source[A])] = this ~> HeadAndTail()
}
