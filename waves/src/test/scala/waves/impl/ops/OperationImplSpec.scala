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
package impl
package ops

import org.reactivestreams.api.{ Consumer, Producer }
import org.reactivestreams.spi.Publisher
import scala.util.control.NoStackTrace
import org.specs2.mutable.Specification
import org.specs2.matcher.Scope
import OperationProcessor.{ SubUpstreamHandling, SubDownstreamHandling }

abstract class OperationImplSpec extends Specification {

  def test[A, B](op: Operation[A, B])(body: Fixture[A, B] ⇒ Unit): Scope = {
    val fixture = new Fixture[A, B] {
      def operation = op
    }
    test(fixture, body)
  }

  def test[F <: Fixture[_, _]](fixture: F, body: F ⇒ Unit): Scope =
    new Scope {
      body(fixture)
      import fixture._
      expectNoRequestMore()
      expectNoCancel()
      expectNoNext()
      expectNoComplete()
      expectNoError()
      expectNoRequestSubUpstream()
      verifiedForCleanExit foreach {
        case Left(substream) ⇒
          substream.expectNoRequestMore()
          substream.expectNoCancel()
        case Right(substream) ⇒
          substream.expectNoNext()
          substream.expectNoComplete()
          substream.expectNoError()
      }
    }

  abstract class Fixture[A, B] extends MockUpstream with MockDownstream {
    def operation: Operation[A, B]

    private[OperationImplSpec] var verifiedForCleanExit = Seq.empty[Either[MockUpstream, MockDownstream]]
    private val chain = new OperationChain(operation, processorContext)
    private var requestSubUpstreamCalls = Seq.empty[(Producer[_ <: Any], () ⇒ SubDownstreamHandling)]

    chain.connectUpstream(upstream)
    chain.connectDownstream(downstream)

    private def processorContext: OperationProcessor.Context =
      new OperationProcessor.Context {
        def requestSubUpstream[T <: Any](producer: Producer[T], subDownstream: ⇒ SubDownstreamHandling): Unit =
          requestSubUpstreamCalls :+= producer -> subDownstream _
        def requestSubDownstream(subUpstream: ⇒ SubUpstreamHandling): Producer[Any] with Downstream =
          new Producer[Any] with Downstream with MockDownstream {
            verifiedForCleanExit :+= Right(this)
            def getPublisher: Publisher[Any] = throw new IllegalStateException // should never be called in a test
            def produceTo(consumer: Consumer[Any]): Unit = throw new IllegalStateException // should never be called in a test
            def onNext(element: Any): Unit = downstream.onNext(element)
            def onComplete(): Unit = downstream.onComplete()
            def onError(cause: Throwable): Unit = downstream.onError(cause)
            def requestMore(counts: Int*): Unit = counts.foreach(subUpstream.subRequestMore)
            def cancel(): Unit = subUpstream.subCancel()
          }
      }

    def expectRequestSubUpstream(producer: Producer[_ <: Any]): SubDownstreamInterface =
      requestSubUpstreamCalls match {
        case Seq((`producer`, subDownstreamHandling)) ⇒
          requestSubUpstreamCalls = Nil
          val mockUpstream = new SubDownstreamInterface(subDownstreamHandling)
          verifiedForCleanExit :+= Left(mockUpstream)
          mockUpstream
        case x ⇒ fail(s"Expected ${callsToString("requestSubUpstream", Seq(producer))} but got " +
          callsToString("requestSubUpstream", x.map(_._1)))
      }
    def expectNoRequestSubUpstream(): Unit =
      if (requestSubUpstreamCalls.nonEmpty)
        fail("Unexpected calls: " + callsToString("requestSubUpstream", requestSubUpstreamCalls.map(_._1)))

    def requestMore(counts: Int*): Unit = counts.foreach(chain.rightUpstream.requestMore)
    def cancel(): Unit = chain.rightUpstream.cancel()

    def onNext(elements: Any*): Unit = elements.foreach(chain.leftDownstream.onNext)
    def onComplete(): Unit = chain.leftDownstream.onComplete()
    def onError(cause: Throwable): Unit = chain.leftDownstream.onError(cause)
  }

  class SubDownstreamInterface(sdh: () ⇒ SubDownstreamHandling) extends MockUpstream {
    def onSubscribe(): Unit = sdh().subOnSubscribe(upstream)
    def onNext(elements: Any*): Unit = elements.foreach(sdh().subOnNext)
    def onComplete(): Unit = sdh().subOnComplete()
    def onError(cause: Throwable): Unit = sdh().subOnError(cause)
  }

  trait MockUpstream {
    protected var requestMoreCalls = Seq.empty[Int]
    protected var cancelCalls = 0
    protected val upstream: Upstream =
      new Upstream {
        def requestMore(elements: Int) = requestMoreCalls :+= elements
        def cancel() = cancelCalls += 1
      }

    def expectRequestMore(counts: Int*): Unit = {
      if (requestMoreCalls != counts)
        fail(s"Expected ${callsToString("requestMore", counts)} but got " + callsToString("requestMore", requestMoreCalls))
      requestMoreCalls = Nil
    }
    def expectCancel(): Unit = {
      if (cancelCalls != 1) fail("Expected one `cancel()` but got " + cancelCalls)
      cancelCalls = 0
    }
    def expectNoRequestMore(): Unit =
      if (requestMoreCalls.nonEmpty) fail("Unexpected calls: " + callsToString("requestMore", requestMoreCalls))
    def expectNoCancel(): Unit =
      if (cancelCalls > 0) fail("Unexpected `cancel()` call")
  }

  trait MockDownstream {
    protected var onNextCalls = Seq.empty[Any]
    protected var onCompleteCalls = 0
    protected var onErrorCalls = Seq.empty[Throwable]
    protected val downstream: Downstream =
      new Downstream {
        def onNext(element: Any): Unit = onNextCalls :+= element
        def onComplete(): Unit = onCompleteCalls += 1
        def onError(cause: Throwable): Unit = onErrorCalls :+= cause
      }

    def expectNextSubProducer(): MockDownstream = onNextCalls match {
      case Seq(x: MockDownstream) ⇒
        onNextCalls = Nil
        x
      case _ ⇒ fail(s"Expected `onNext(Flow)` but got " + callsToString("onNext", onNextCalls))
    }
    def expectNext(values: Any*): Unit = {
      if (onNextCalls != values)
        fail(s"Expected ${callsToString("onNext", values)} but got " + callsToString("onNext", onNextCalls))
      onNextCalls = Nil
    }
    def expectComplete(): Unit = {
      if (onCompleteCalls != 1) fail("Expected one `onComplete()` but got " + onCompleteCalls)
      onCompleteCalls = 0
    }
    def expectError(causes: Throwable*): Unit = {
      if (onErrorCalls != causes)
        fail(s"Expected ${callsToString("onError", causes)} but got " + callsToString("onError", onErrorCalls))
      onErrorCalls = Nil
    }
    def expectNoNext(): Unit =
      if (onNextCalls.nonEmpty) fail("Unexpected calls: " + callsToString("onNext", onNextCalls))
    def expectNoComplete(): Unit =
      if (onCompleteCalls > 0) fail("Unexpected `onComplete()` call")
    def expectNoError(): Unit =
      if (onErrorCalls.nonEmpty) fail("Unexpected calls: " + callsToString("onError", onErrorCalls))

    def requestMore(counts: Int*): Unit
    def cancel(): Unit
  }

  def mockProducer[T] =
    new Producer[T] {
      def getPublisher: Publisher[T] = throw new IllegalStateException // should never be called in a test
      def produceTo(consumer: Consumer[T]): Unit = throw new IllegalStateException // should never be called in a test
    }

  case class TestException(msg: String) extends RuntimeException(msg) with NoStackTrace
  object TestException extends TestException("TEST")

  // helpers
  private def callsToString[T](method: String, calls: Seq[T]) =
    if (calls.isEmpty) "nothing" else calls.mkString(method + '(', s"), $method(", ")")

  private def fail(msg: String) = failure(msg).asInstanceOf[Nothing]
}