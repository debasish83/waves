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

package waves.impl

import org.reactivestreams.api.Producer
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise, Await }
import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions
import org.specs2.matcher.Matcher
import waves.Operation.Split
import waves.{ StreamProducer, Flow }

class ExamplesSpec extends Specification with NoTimeConversions {
  import scala.concurrent.ExecutionContext.Implicits.global

  "The OperationImpl infrastructure must properly execute" >> {

    "concat" in {
      flow(1 to 10).concat(flow(11 to 20).toProducer) must produce(1 to 20)
    }

    "collect" in {
      flow(1 to 10).collect { case x if x % 3 == 0 ⇒ x } must produce(3, 6, 9)
    }

    "concatAll" in {
      flow(flow(1 to 10).toProducer, flow(11 to 20).toProducer).concatAll must produce(1 to 20)
    }

    "drop" in {
      flow(1 to 10).drop(7) must produce(8 to 10)
    }

    "exists" in {
      flow(1 to 10).exists(_ == 7) must produce(true)
    }

    "filter" in {
      flow(1 to 20).filter(_ % 2 == 0) must produce(2 to 20 by 2)
    }

    "find" in {
      flow(1 to 10).find(_ == 7) must produce(7)
    }

    "fold" in {
      flow(1 to 10).fold(1000)(_ + _) must produce(1055)
    }

    "forAll" in {
      flow(1 to 10).forAll(_ < 10) must produce(false)
    }

    "groupedIntoSeqs" in {
      flow(1 to 10).groupedIntoSeqs(4) must produce(Seq(1, 2, 3, 4), Seq(5, 6, 7, 8), Seq(9, 10))
    }

    "grouped" in {
      flow(1 to 10).grouped(4)
        .mapConcat(Flow(_).fold("")(_ + _.toString).toProducer) must produce("1234", "5678", "910")
    }

    //    "head" in {
    //      flow(1 to 10).grouped(4).head must produce(1 to 4)
    //    }

    "headAndTail" in {
      flow(1 to 10).grouped(3).headAndTail
        .mapConcat {
          case (head, tail) ⇒ Flow(tail).drainToSeq.map(tailSeq ⇒ List(head -> tailSeq))
        } must produce(1 -> Seq(2, 3), 4 -> Seq(5, 6), 7 -> Seq(8, 9), 10 -> Nil)
    }

    "map" in {
      flow(1 to 10).map(_ * 2) must produce(2 to 20 by 2)
    }

    "mapConcat" in {
      flow(0 to 10 by 2).mapConcat(x ⇒ Seq(x, x + 1)) must produce(0 to 11)
    }

    "merge" in {
      flow(1 to 10).merge(Flow(11 to 20).toProducer).reduce(_ + _) must produce(210)
    }

    "mergeToEither" in {
      flow(1, 2).mergeToEither(flow(3, 4).toProducer)
        .map(_.toString) must produceSorted("Left(3)", "Left(4)", "Right(1)", "Right(2)")
    }

    "multiply" in {
      flow('A' to 'F').drop(2).multiply(3) must produce('C', 'C', 'C', 'D', 'D', 'D', 'E', 'E', 'E', 'F', 'F', 'F')
      flow('A' to 'F').drop(2).multiply(3).fold("")(_ + _) must produce("CCCDDDEEEFFF")
    }

    "partition" in {
      val promise = Promise[Producer[Int]]()
      flow(1 to 8)
        .partition(promise.success)(i ⇒ Either.cond(i % 2 != 0, "x" + i, i))
        .buffer(2)
        .zip(StreamProducer(promise.future)) must produce("x1" -> 2, "x3" -> 4, "x5" -> 6, "x7" -> 8)
    }

    "recover" in {
      flow(4 to 0 by -1).map(24 / _).recover[Any, List[Any]] { case _: ArithmeticException ⇒ List("n/a") } must produce(6, 8, 12, 24, "n/a")
    }

    "reduce" in {
      flow(1 to 4).reduce(_ * _) must produce(24)
    }

    "recover" in {
      flow(3 to 0 by -1).map(24 / _).tryRecover.map(_.toString) must produce("Success(8)", "Success(12)", "Success(24)", "Failure(java.lang.ArithmeticException: / by zero)")
    }

    "split" in {
      flow(1 to 10)
        .split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
        .mapConcat(Flow(_).fold("")(_ + _.toString).toProducer) must produce("123", "4567", "8910")
    }

    "tail" in {
      flow(1 to 10).tail must produce(2 to 10)
    }

    "take" in {
      flow(1 to 10).take(4) must produce(1 to 4)
    }

    "tee" in {
      val promise = Promise[Producer[Int]]()
      val test1 = Future(flow(1 to 10).tee(promise.success) must produce(1 to 10))
      val test2 = Flow(promise.future) must produce(1 to 10)
      Await.result(test1, 100.millis) and test2
    }

    "takeWhile" in {
      flow(1 to 10).takeWhile(_ < 7) must produce(1 to 6)
    }

    "unzip" in {
      val promise = Promise[Producer[Int]]()
      val test1 = Future(flow((1 to 4) zip (5 to 8)).unzip(promise.success) must produce(1 to 4))
      val test2 = Flow(promise.future) must produce(5 to 8)
      Await.result(test1, 100.millis) and test2
    }

    "zip" in {
      flow(1 to 4).zip(flow(11 to 15).toProducer) must produce(1 -> 11, 2 -> 12, 3 -> 13, 4 -> 14)
    }

    //////////////////////////////////////////////////////////////

    "a simple operations chain (example 1)" in {
      flow(1 to 4) must produce(1 to 4)
      Flow(flow(1 to 4).toProducer) must produce(1 to 4)
      Flow(flow(1 to 4).map(_ * 2).toProducer) must produce(2 to 8 by 2)
      flow(1 to 20).filter(_ % 2 == 0) must produce(2 to 20 by 2)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3) must produce(6 to 60 by 6)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3).take(5) must produce(6 to 30 by 6)
    }

    "an operation chain that would overflow the stack if it did not provide for re-entrancy support" in {
      flow(1 to 2).multiply(10000).fold(0)(_ + _) must produce(30000)
    }

    "custom operations" in {
      def splitAt4 = operation[Int].split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
      def substreamsToString[T] = operation[Producer[T]].mapConcat(Flow(_).fold("")(_ + _.toString).toProducer)

      flow(1 to 10).op(splitAt4).op(substreamsToString) must produce("123", "4567", "8910")
    }
  }

  ///////////////////////////////////////////////////////////

  def flow[T](first: T, more: T*): Flow[T] = flow(first +: more)
  def flow[T](iterable: Iterable[T]): Flow[T] = Flow(iterable)

  def produce[T](first: T, more: T*): Matcher[Flow[T]] = produce(first +: more)
  def produce[T](expected: Seq[T]) =
    beEqualTo(expected) ^^ { flow: Flow[T] ⇒ drainFlow(flow) }

  def produceSorted[T](first: T, more: T*): Matcher[Flow[T]] = produceSorted(first +: more)
  def produceSorted[T](expected: Seq[T]) =
    beEqualTo(expected) ^^ { flow: Flow[T] ⇒ drainFlow(flow).sortBy(_.toString) }

  def drainFlow[T](flow: Flow[T]) = Await.result(flow.drainToSeq, 100.millis)
}
