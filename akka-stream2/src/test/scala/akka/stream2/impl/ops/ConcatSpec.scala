package akka.stream2.impl.ops

import akka.stream2.Operation

class ConcatSpec extends OperationImplSpec {

  val producer2 = mockProducer[Char]
  val op = Operation.Concat[Char](producer2)

  "`Concat` should" - {

    "before completion of first upstream" - {

      "propagate requestMore" in new Test(op) {
        requestMore(5)
        expectRequestMore(5)
        requestMore(2)
        expectRequestMore(2)
      }

      "propagate cancel" in new Test(op) {
        cancel()
        expectCancel()
      }

      "not propagate complete" in new Test(op) {
        onComplete()
        expectNoComplete()
        expectRequestSubUpstream(producer2)
      }

      "propagate error" in new Test(op) {
        onError(TestException)
        expectError(TestException)
      }

      "forward elements to downstream" in new Test(op) {
        requestMore(1, 2)
        expectRequestMore(1, 2)
        onNext('A', 'B', 'C')
        expectNext('A', 'B', 'C')
      }
    }

    "while waiting for subscription to second upstream" - {
      def playToWaitingForSubscription[A, B](test: Test[A, B]) = {
        import test._
        requestMore(1)
        expectRequestMore(1)
        onComplete()
        expectRequestSubUpstream(producer2)
      }

      "gather up requestMore calls from downstream" in new Test(op) {
        val upstream2 = playToWaitingForSubscription(this)
        requestMore(2)
        requestMore(1) // now the downstream has requested a total of 4 elements
        upstream2.onSubscribe()
        upstream2.expectRequestMore(4)
      }

      "immediately complete downstream if second upstream is completed before subscription" in new Test(op) {
        val upstream2 = playToWaitingForSubscription(this)
        upstream2.onComplete()
        expectComplete()
      }

      "when receiving an error from the second upstream" - {
        "immediately propagate error to downstream if downstream has not yet cancelled" in new Test(op) {
          val upstream2 = playToWaitingForSubscription(this)
          upstream2.onError(TestException)
          expectError(TestException)
        }
        "ignore error if downstream already cancelled" in new Test(op) {
          val upstream2 = playToWaitingForSubscription(this)
          cancel()
          upstream2.onError(TestException)
          expectNoError()
        }
      }

      "when cancelled from downstream" - {
        "eventually cancel second upstream" in new Test(op) {
          val upstream2 = playToWaitingForSubscription(this)
          cancel()
          upstream2.onSubscribe()
          upstream2.expectCancel()
        }
        "don't cancel second upstream if it is empty" in new Test(op) {
          val upstream2 = playToWaitingForSubscription(this)
          cancel()
          upstream2.onComplete()
          upstream2.expectNoCancel()
        }
      }
    }

    "after subscription to second upstream" - {
      def playToAfterSubscriptionToSecondUpstream[A, B](test: Test[A, B]) = {
        import test._
        requestMore(1)
        expectRequestMore(1)
        onComplete()
        val upstream2 = expectRequestSubUpstream(producer2)
        upstream2.onSubscribe()
        upstream2.expectRequestMore(1)
        upstream2
      }

      "propagate requestMore" in new Test(op) {
        val upstream2 = playToAfterSubscriptionToSecondUpstream(this)
        requestMore(5)
        upstream2.expectRequestMore(5)
        requestMore(2)
        upstream2.expectRequestMore(2)
      }

      "propagate cancel" in new Test(op) {
        val upstream2 = playToAfterSubscriptionToSecondUpstream(this)
        cancel()
        upstream2.expectCancel()
      }

      "propagate complete" in new Test(op) {
        val upstream2 = playToAfterSubscriptionToSecondUpstream(this)
        upstream2.onComplete()
        expectComplete()
      }

      "propagate error" in new Test(op) {
        val upstream2 = playToAfterSubscriptionToSecondUpstream(this)
        upstream2.onError(TestException)
        expectError(TestException)
      }

      "forward elements to downstream" in new Test(op) {
        val upstream2 = playToAfterSubscriptionToSecondUpstream(this)
        requestMore(3)
        upstream2.expectRequestMore(3)
        upstream2.onNext('A', 'B', 'C')
        expectNext('A', 'B', 'C')
      }
    }
  }
}
