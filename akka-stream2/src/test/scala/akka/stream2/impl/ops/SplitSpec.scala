package akka.stream2.impl.ops

import akka.stream2.Operation
import akka.stream2.Operation.Split._

class SplitSpec extends OperationImplSpec {

  val op = Operation.Split[String] {
    case x if x startsWith "DROP"  ⇒ Drop
    case x if x startsWith "LAST"  ⇒ Last
    case x if x startsWith "FIRST" ⇒ First
    case _                         ⇒ Append
  }

  "`Split` should" - {

    "request one from upstream on first requestMore from downstream" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(1)
      requestMore(2)
      expectNoRequestMore()
    }

    "while waiting for the first element from upstream" - {
      "not start a new substream if `f` returns `Drop`" in test(op) { fixture ⇒
        import fixture._
        requestMore(2)
        expectRequestMore(1)
        onNext("DROP")
        expectRequestMore(1)
      }
      for (command ← Seq("Append", "Last", "First")) {
        s"start a new substream if `f` returns `$command`" in test(op) { fixture ⇒
          import fixture._
          requestMore(2)
          expectRequestMore(1)
          onNext(command.toUpperCase)
          expectNextSubProducer()
        }
      }
      "propagate cancel" in test(op) { fixture ⇒
        import fixture._
        fixture.cancel()
        expectCancel()
      }
      "propagate complete" in test(op) { fixture ⇒
        import fixture._
        onComplete()
        expectComplete()
      }
      "propagate error" in test(op) { fixture ⇒
        import fixture._
        onError(TestException)
        expectError(TestException)
      }
    }

    "while waiting for the first requestMore from a sub-stream" - {
      def playToConsumingSubstream[A, B](firstElement: A, fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onNext(firstElement)
        expectNextSubProducer()
      }

      "gather up requestMore calls from the main downstream" in test(op) { fixture ⇒
        val sub1 = playToConsumingSubstream("LAST 1", fixture)
        import fixture._
        requestMore(2) // main downstream requestMore
        sub1.requestMore(2)
        sub1.expectNext("LAST 1")
        sub1.expectComplete()
        expectRequestMore(1)
        onNext("LAST 2")
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("LAST 2")
        sub2.expectComplete()
        expectRequestMore(1)
        onNext("LAST 3")
        val sub3 = expectNextSubProducer()
        sub3.requestMore(5)
        sub3.expectNext("LAST 3")
        sub3.expectComplete()
      }

      "immediately propagate cancel from main downstream to upstream and eventually complete sub-stream" in test(op) { fixture ⇒
        val sub = playToConsumingSubstream("foo", fixture)
        import fixture._
        fixture.cancel() // main downstream cancel
        expectCancel()
        sub.requestMore(1)
        sub.expectNext("foo")
        sub.expectComplete()
      }

      "immediately propagate completion from upstream to downstream and eventually" - {
        for (cmd ← Seq("Append", "Last")) {
          s"complete sub-stream if command for element was `$cmd`" in test(op) { fixture ⇒
            val sub = playToConsumingSubstream(cmd.toUpperCase, fixture)
            import fixture._
            onComplete()
            expectComplete()
            sub.requestMore(1)
            sub.expectNext(cmd.toUpperCase)
            sub.expectComplete()
          }
        }
      }

      "immediately propagate error from upstream to downstream and eventually" - {
        "propagate error to sub-stream if the element was not already marked as the last in sub-stream" in test(op) { fixture ⇒
          val sub = playToConsumingSubstream("foo", fixture)
          import fixture._
          onError(TestException)
          expectError(TestException)
          sub.requestMore(1)
          sub.expectNext("foo")
          sub.expectError(TestException)
        }
        "complete sub-stream if the element was marked as the last in sub-stream" in test(op) { fixture ⇒
          val sub = playToConsumingSubstream("LAST", fixture)
          import fixture._
          onError(TestException)
          expectError(TestException)
          sub.requestMore(1)
          sub.expectNext("LAST")
          sub.expectComplete()
        }
      }

      "push back the first element and re-apply `f` if the sub-stream is cancelled before the first requestMore" in test(op) { fixture ⇒
        val sub1 = playToConsumingSubstream("foo", fixture)
        import fixture._
        sub1.cancel()
        expectNoNext()
        requestMore(1)
        expectNoRequestMore()
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("foo")
      }
    }

    "while producing into a sub-stream" - {
      def playToProducingIntoSubstream[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onNext("foo")
        val sub = expectNextSubProducer()
        sub.requestMore(1)
        sub.expectNext("foo")
        sub
      }

      "gather up requestMore calls from the main downstream" in test(op) { fixture ⇒
        val sub = playToProducingIntoSubstream(fixture)
        import fixture._
        sub.requestMore(1)
        expectRequestMore(1)
        requestMore(1) // main downstream requestMore
        onNext("LAST")
        sub.expectNext("LAST")
        sub.expectComplete()
        expectRequestMore(1)
      }

      "when main downstream is cancelled" - {
        "continue the current sub-stream until completion before canceling upstream" - {
          "when sub-stream is ended with `Last`" in test(op) { fixture ⇒
            val sub = playToProducingIntoSubstream(fixture)
            import fixture._
            sub.requestMore(5)
            expectRequestMore(1)
            fixture.cancel() // main downstream cancel
            onNext("bar")
            sub.expectNext("bar")
            expectRequestMore(1)
            onNext("LAST")
            sub.expectNext("LAST")
            sub.expectComplete()
            expectCancel()
          }
          "when sub-stream is ended with `First`" in test(op) { fixture ⇒
            val sub = playToProducingIntoSubstream(fixture)
            import fixture._
            sub.requestMore(2)
            expectRequestMore(1)
            fixture.cancel() // main downstream cancel
            onNext("bar")
            sub.expectNext("bar")
            expectRequestMore(1)
            onNext("FIRST")
            sub.expectComplete()
            expectCancel()
          }
        }

        "cancel upstream upon cancellation of sub-stream" in test(op) { fixture ⇒
          val sub = playToProducingIntoSubstream(fixture)
          import fixture._
          sub.requestMore(1)
          expectRequestMore(1)
          fixture.cancel() // main downstream cancel
          onNext("bar")
          sub.expectNext("bar")
          sub.cancel()
          expectCancel()
        }
      }

      "drop upstream element if `f` returns `Drop`" in test(op) { fixture ⇒
        val sub = playToProducingIntoSubstream(fixture)
        import fixture._
        sub.requestMore(2)
        expectRequestMore(1)
        onNext("DROP")
        expectRequestMore(1)
        onNext("baz")
        sub.expectNext("baz")
        expectRequestMore(1)
      }

      "append upstream element if `f` returns `Append`" in test(op) { fixture ⇒
        val sub = playToProducingIntoSubstream(fixture)
        import fixture._
        sub.requestMore(2)
        expectRequestMore(1)
        onNext("bar")
        sub.expectNext("bar")
        expectRequestMore(1)
        onNext("baz")
        sub.expectNext("baz")
      }

      "append upstream element, complete sub-stream and start new sub-stream if `f` returns `Last`" in test(op) { fixture ⇒
        val sub1 = playToProducingIntoSubstream(fixture)
        import fixture._
        sub1.requestMore(2)
        expectRequestMore(1)
        onNext("LAST")
        sub1.expectNext("LAST")
        sub1.expectComplete()
        requestMore(1)
        expectRequestMore(1)
        onNext("baz")
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("baz")
      }

      "complete sub-stream and start new sub-stream with current element if `f` returns `First`" in test(op) { fixture ⇒
        val sub1 = playToProducingIntoSubstream(fixture)
        import fixture._
        sub1.requestMore(2)
        expectRequestMore(1)
        onNext("FIRST")
        sub1.expectComplete()
        requestMore(1)
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("FIRST")
      }

      "propagate completion from upstream to sub-stream and main downstream" in test(op) { fixture ⇒
        val sub = playToProducingIntoSubstream(fixture)
        import fixture._
        onComplete()
        sub.expectComplete()
        expectComplete()
      }

      "propagate error from upstream to sub-stream and main downstream" in test(op) { fixture ⇒
        val sub = playToProducingIntoSubstream(fixture)
        import fixture._
        onError(TestException)
        sub.expectError(TestException)
        expectError(TestException)
      }

      "start new sub-stream upon cancellation of current sub-stream" in test(op) { fixture ⇒
        val sub1 = playToProducingIntoSubstream(fixture)
        import fixture._
        sub1.cancel()
        expectNoRequestMore()
        requestMore(1)
        expectRequestMore(1)
        onNext("bar")
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("bar")
      }
    }

    "while waiting for requestMore from main downstream with an already available first sub-stream element" - {
      def playToWaitingForRequestMoreFromMainDownstream[A, B](fixture: Fixture[A, B]): Unit = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onNext("foo")
        val sub = expectNextSubProducer()
        sub.requestMore(5)
        sub.expectNext("foo")
        expectRequestMore(1)
        onNext("FIRST")
        sub.expectComplete()
      }

      "only propagate upstream completion after having flushed next sub-stream" in test(op) { fixture ⇒
        playToWaitingForRequestMoreFromMainDownstream(fixture)
        import fixture._
        onComplete()
        expectNoComplete()
        requestMore(1)
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("FIRST")
        sub2.expectComplete()
        expectComplete()
      }

      "only propagate upstream error after having flushed next sub-stream" in test(op) { fixture ⇒
        playToWaitingForRequestMoreFromMainDownstream(fixture)
        import fixture._
        onError(TestException)
        expectNoError()
        requestMore(1)
        val sub2 = expectNextSubProducer()
        sub2.requestMore(1)
        sub2.expectNext("FIRST")
        sub2.expectError(TestException)
        expectError(TestException)
      }
    }

    val op2 = Operation.Split[Long](x ⇒ if (x % 3 == 0) First else Append)
    "properly run example op2" in test(op2) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(1)
      onNext(1L)
      val sub1 = expectNextSubProducer()
      sub1.requestMore(5)
      sub1.expectNext(1L)
      expectRequestMore(1)
      onNext(2L)
      sub1.expectNext(2L)
      expectRequestMore(1)
      onNext(3L)
      sub1.expectComplete()
      val sub2 = expectNextSubProducer()
      sub2.requestMore(5)
      sub2.expectNext(3L)
      expectRequestMore(1)
      onNext(4L)
      sub2.expectNext(4L)
      expectRequestMore(1)
      onNext(5L)
      sub2.expectNext(5L)
      expectRequestMore(1)
      onNext(6L)
      sub2.expectComplete()
      val sub3 = expectNextSubProducer()
      sub3.requestMore(5)
      sub3.expectNext(6L)
      expectRequestMore(1)
      onNext(7L)
      sub3.expectNext(7L)
      expectRequestMore(1)
      onComplete()
      sub3.expectComplete()
      expectComplete()
    }
  }
}
