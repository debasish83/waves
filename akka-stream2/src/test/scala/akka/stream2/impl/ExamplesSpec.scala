package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import scala.collection.immutable.VectorBuilder
import scala.annotation.tailrec
import org.reactivestreams.spi.Publisher
import org.scalatest.matchers.Matcher
import akka.stream2.impl.OperationProcessor
import akka.actor.ActorSystem
import akka.stream2._
import org.scalatest._
import Operation.Split

class ExamplesSpec(override val system: ActorSystem) extends TestEnvironment(Timeouts.defaultTimeoutMillis(system))
  with FreeSpecLike with Matchers with WithActorSystemScalatest {
  implicit def refFactory = system
  import system.dispatcher

  "The OperationImpl infrastructure should properly execute" - {

    "a simple operations chain (example 1)" in {
      source(1 to 20).filter(_ % 2 == 0) should produce(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
      source(1 to 20).filter(_ % 2 == 0).map(_ * 3) should produce(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)
      source(1 to 20).filter(_ % 2 == 0).map(_ * 3).take(5) should produce(6, 12, 18, 24, 30)
    }

    "a simple operations chain (example 2)" in {
      source('A' to 'F').drop(2) should produce('C' to 'F')
      source('A' to 'F').drop(2).multiply(3) should produce('C', 'C', 'C', 'D', 'D', 'D', 'E', 'E', 'E', 'F', 'F', 'F')
      source('A' to 'F').drop(2).multiply(3).fold("")(_ + _) should produce("CCCDDDEEEFFF")
    }

    "an operation chain that would overflow the stack if it did not provide for re-entrancy support" in {
      source(1 to 2).multiply(10000).fold(0)(_ + _) should produce(30000)
    }

    "append" in {
      source(1 to 10).concat(source(11 to 20)) should produce(1 to 20)
    }

    "flatten" in {
      source(source(1 to 10), source(11 to 20)).flatten should produce(1 to 20)
    }

    "split" in {
      source(1 to 10)
        .split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
        .flatMap(_.fold("")(_ + _.toString)) should produce("123", "4567", "8910")
    }

    "custom operations" in {
      def splitAt4 = operation[Int].split(x ⇒ if (x % 4 == 0) Split.First else Split.Append)
      def substreamsToString[T] = operation[Source[T]].flatMap(_.fold("")(_ + _.toString))

      source(1 to 10).op(splitAt4).op(substreamsToString) should produce("123", "4567", "8910")
    }
  }

  ///////////////////////////////////////////////////////////

  def source[T](first: T, more: T*): Source[T] = source(first +: more)
  def source[T](iterable: Iterable[T]): Source[T] = IteratorProducer(iterable)

  def produce[T](first: T, more: T*): Matcher[Source[T]] = produce(first +: more)
  def produce[T](expected: Seq[T]) =
    equal(expected).matcher[Seq[T]] compose { source: Source[T] ⇒
      val Source.Mapped(prod, op) = source
      val processor = new OperationProcessor(op)
      prod.getPublisher.subscribe(processor.getSubscriber)
      drain(processor.getPublisher)
    }

  def drain[T](pub: Publisher[T]): Seq[T] = {
    val sub = newManualSubscriber(pub)
    val builder = new VectorBuilder[T]
    @tailrec def pull(): Seq[T] = {
      sub.requestMore(1)
      val element = sub.nextElementOrEndOfStream(100) // TODO: remove timeout param
      if (element.isDefined) {
        builder += element.get
        pull()
      } else {
        verifyNoAsyncErrors()
        builder.result()
      }
    }
    pull()
  }
}
