package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import org.reactivestreams.api.Producer
import org.scalatest.matchers.Matcher
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorSystem
import akka.stream2._
import org.scalatest._
import Operation.Split

class ExamplesSpec extends FreeSpec with Matchers with BeforeAndAfterAll {
  val testConf: Config = ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    akka.loglevel = WARNING""")
  implicit val system = ActorSystem(getClass.getSimpleName, testConf)
  override protected def afterAll(): Unit = system.shutdown()
  import system.dispatcher

  "The OperationImpl infrastructure should properly execute" - {

    "a simple operations chain (example 1)" in {
      flow(1 to 4) should produce(1, 2, 3, 4)
      flow(1 to 20).filter(_ % 2 == 0) should produce(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3) should produce(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)
      flow(1 to 20).filter(_ % 2 == 0).map(_ * 3).take(5) should produce(6, 12, 18, 24, 30)
    }

    "a simple operations chain (example 2)" in {
      flow('A' to 'F').drop(2) should produce('C' to 'F')
      flow('A' to 'F').drop(2).multiply(3) should produce('C', 'C', 'C', 'D', 'D', 'D', 'E', 'E', 'E', 'F', 'F', 'F')
      flow('A' to 'F').drop(2).multiply(3).fold("")(_ + _) should produce("CCCDDDEEEFFF")
    }

    "an operation chain that would overflow the stack if it did not provide for re-entrancy support" in {
      flow(1 to 2).multiply(10000).fold(0)(_ + _) should produce(30000)
    }

    "append" in {
      flow(1 to 10).concat(flow(11 to 20).toProducer) should produce(1 to 20)
    }

    "concatAll" in {
      flow(flow(1 to 10).toProducer, flow(11 to 20).toProducer).concatAll should produce(1 to 20)
    }

    "split" in {
      flow(1 to 10)
        .split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
        .mapConcat(Flow(_).fold("")(_ + _.toString).toProducer) should produce("123", "4567", "8910")
    }

    "headAndTail" in {
      flow(flow(1 to 4).toProducer, flow(7 to 9).toProducer)
        .headAndTail
        .mapConcat {
          case (head, tail) ⇒ Flow(tail).drainToSeq.map(head -> _)
        } should produce(1 -> Seq(2, 3, 4), 7 -> Seq(8, 9))
    }

    "custom operations" in {
      def splitAt4 = operation[Int].split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
      def substreamsToString[T] = operation[Producer[T]].mapConcat(Flow(_).fold("")(_ + _.toString).toProducer)

      flow(1 to 10).op(splitAt4).op(substreamsToString) should produce("123", "4567", "8910")
    }
  }

  ///////////////////////////////////////////////////////////

  def flow[T](first: T, more: T*): Flow[T] = flow(first +: more)
  def flow[T](iterable: Iterable[T]): Flow[T] = Flow(iterable)

  def produce[T](first: T, more: T*): Matcher[Flow[T]] = produce(first +: more)
  def produce[T](expected: Seq[T]) =
    equal(expected).matcher[Seq[T]] compose { flow: Flow[T] ⇒ Await.result(flow.drainToSeq, 100.millis) }
}
