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
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import scala.concurrent.Await
import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions
import org.specs2.matcher.Matcher
import akka.actor.ActorSystem
import waves.Operation.Split
import waves.Flow

class ExamplesSpec extends Specification with NoTimeConversions {
  val testConf: Config = ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    akka.loglevel = WARNING""")
  implicit val system = ActorSystem(getClass.getSimpleName, testConf)
  import system.dispatcher

  "The OperationImpl infrastructure must properly execute" >> {

    "a simple operations chain (example 1)" in {
      flow(1 to 4) must produce(1, 2, 3, 4)
      Flow(flow(1 to 4).toProducer) must produce(1, 2, 3, 4)
      Flow(flow(1 to 4).map(_ * 2).toProducer) must produce(2, 4, 6, 8)
      flow(1 to 20).filter(_ % 2 == 0) must produce(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3) must produce(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3).take(5) must produce(6, 12, 18, 24, 30)
    }

    "a simple operations chain (example 2)" in {
      flow('A' to 'F').drop(2) must produce('C' to 'F')
      flow('A' to 'F').drop(2).multiply(3) must produce('C', 'C', 'C', 'D', 'D', 'D', 'E', 'E', 'E', 'F', 'F', 'F')
      flow('A' to 'F').drop(2).multiply(3).fold("")(_ + _) must produce("CCCDDDEEEFFF")
    }

    "an operation chain that would overflow the stack if it did not provide for re-entrancy support" in {
      flow(1 to 2).multiply(10000).fold(0)(_ + _) must produce(30000)
    }

    "append" in {
      flow(1 to 10).concat(flow(11 to 20).toProducer) must produce(1 to 20)
    }

    "concatAll" in {
      flow(flow(1 to 10).toProducer, flow(11 to 20).toProducer).concatAll must produce(1 to 20)
    }

    "split" in {
      flow(1 to 10)
        .split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
        .mapConcat(Flow(_).fold("")(_ + _.toString).toProducer) must produce("123", "4567", "8910")
    }

    "headAndTail" in {
      flow(flow(1 to 4).toProducer, flow(7 to 9).toProducer)
        .headAndTail
        .mapConcat {
          case (head, tail) ⇒ Flow(tail).drainToSeq.map(head -> _)
        } must produce(1 -> Seq(2, 3, 4), 7 -> Seq(8, 9))
    }

    "custom operations" in {
      def splitAt4 = operation[Int].split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
      def substreamsToString[T] = operation[Producer[T]].mapConcat(Flow(_).fold("")(_ + _.toString).toProducer)

      flow(1 to 10).op(splitAt4).op(substreamsToString) must produce("123", "4567", "8910")
    }
  }

  step(system.shutdown())

  ///////////////////////////////////////////////////////////

  def flow[T](first: T, more: T*): Flow[T] = flow(first +: more)
  def flow[T](iterable: Iterable[T]): Flow[T] = Flow(iterable)

  def produce[T](first: T, more: T*): Matcher[Flow[T]] = produce(first +: more)
  def produce[T](expected: Seq[T]) =
    beEqualTo(expected) ^^ { flow: Flow[T] ⇒ Await.result(flow.drainToSeq, 100.millis) }
}
