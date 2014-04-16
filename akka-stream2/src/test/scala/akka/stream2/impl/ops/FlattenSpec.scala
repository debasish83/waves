package akka.stream2.impl.ops

import akka.stream2.Operation

class FlattenSpec extends OperationImplSpec {

  val op = Operation.Flatten[Char]()

  "`Flatten` should" - {

    "request one from super stream on first requestMore from downstream" in new Test(op) {
      requestMore(5)
      expectRequestMore(1)
      requestMore(2)
      expectNoRequestMore()
    }

    "while waiting for sub-stream subscription" - {
      def playToWaitingForSubstreamSubscription[A, B](test: Test[A, B]) = {
        import test._
        requestMore(1)
        expectRequestMore(1)
        val flowA = mockFlow
        onNext(flowA)
        expectRequestSubUpstream(flowA)
      }

      "gather up requestMore calls from downstream" in new Test(op) {
        val subUpstream = playToWaitingForSubstreamSubscription(this)
        requestMore(2, 3)
        subUpstream.onSubscribe()
        subUpstream.expectRequestMore(6)
      }

      "properly handle empty sub streams" in new Test(op) {
        val subUpstream = playToWaitingForSubstreamSubscription(this)
        subUpstream.onComplete()
        expectRequestMore(1)
      }

      "when receiving an error from the sub-stream" - {
        "immediately propagate error to downstream and cancel super upstream if not yet cancelled" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          subUpstream.onError(TestException)
          expectError(TestException)
          expectCancel()
        }
        "ignore error if downstream already cancelled" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          cancel()
          expectCancel()
          subUpstream.onError(TestException)
          expectNoError()
          expectNoCancel()
        }
      }

      "when cancelled from downstream" - {
        "immediately cancel super stream and eventually cancel non-empty sub stream" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          cancel()
          expectCancel()
          subUpstream.onSubscribe()
          subUpstream.expectCancel()
        }
        "immediately cancel super stream and ignore empty sub stream" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          cancel()
          expectCancel()
          subUpstream.onComplete()
        }
      }

      "remember onComplete from super stream and complete after" - {
        "non-empty sub stream completion" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          onComplete()
          subUpstream.onSubscribe()
          subUpstream.expectRequestMore(1)
          subUpstream.onNext('A')
          expectNext('A')
          requestMore(2)
          subUpstream.expectRequestMore(2)
          subUpstream.onNext('B')
          expectNext('B')
          subUpstream.onComplete()
          expectComplete()
        }
        "empty sub stream completion" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          onComplete()
          subUpstream.onComplete()
          expectComplete()
        }
      }

      "remember onError from super stream and propagate after" - {
        "non-empty sub stream completion" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          onError(TestException)
          subUpstream.onSubscribe()
          subUpstream.expectRequestMore(1)
          subUpstream.onNext('A')
          expectNext('A')
          expectNoError()
          subUpstream.onComplete()
          expectError(TestException)
        }
        "empty sub stream completion" in new Test(op) {
          val subUpstream = playToWaitingForSubstreamSubscription(this)
          onError(TestException)
          subUpstream.onComplete()
          expectError(TestException)
        }
      }
    }

    "while consuming substream" - {
      def playToConsumingSubstream[A, B](test: Test[A, B]) = {
        import test._
        requestMore(1)
        expectRequestMore(1)
        val flowA = mockFlow
        onNext(flowA)
        val subUpstream = expectRequestSubUpstream(flowA)
        subUpstream.onSubscribe()
        subUpstream.expectRequestMore(1)
        subUpstream
      }

      "request elements from sub stream and produce to downstream" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        requestMore(1)
        subUpstream.expectRequestMore(1) // one other requestMore has already been received in `playToConsumingSubstream`
        subUpstream.onNext('A', 'B')
        expectNext('A', 'B')
        subUpstream.expectNoRequestMore()
        requestMore(3)
        subUpstream.expectRequestMore(3)
        subUpstream.onNext('C', 'D', 'E')
        expectNext('C', 'D', 'E')
        subUpstream.expectNoRequestMore()
      }

      "request next from super stream when sub stream is completed and properly carry over requested count" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.expectNoRequestMore()
        requestMore(5)
        subUpstream.expectRequestMore(5)
        subUpstream.onComplete()
        expectRequestMore(1)
        val flowB = mockFlow
        onNext(flowB)
        val subUpstream2 = expectRequestSubUpstream(flowB)
        subUpstream2.onSubscribe()
        subUpstream2.expectRequestMore(5)
      }

      "continue to produce to downstream when super stream completes and complete upon sub stream completion" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.expectNoRequestMore()
        onComplete()
        requestMore(1)
        subUpstream.expectRequestMore(1)
        subUpstream.onNext('B')
        expectNext('B')
        subUpstream.onComplete()
        expectComplete()
      }

      "continue to produce to downstream when super stream errors and propagate upon sub stream completion" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.expectNoRequestMore()
        onError(TestException)
        requestMore(1)
        subUpstream.expectRequestMore(1)
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.onComplete()
        expectError(TestException)
      }

      "cancel sub stream and main stream when cancelled from downstream" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        subUpstream.onNext('A')
        expectNext('A')
        cancel()
        subUpstream.expectCancel()
        expectCancel()
      }

      "immediately propagate sub stream error and cancel super stream" in new Test(op) {
        val subUpstream = playToConsumingSubstream(this)
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.onError(TestException)
        expectError(TestException)
        expectCancel()
      }
    }
  }
}
