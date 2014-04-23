package akka.stream2
package impl
package ops

import scala.util.DynamicVariable

class TeeSpec extends OperationImplSpec {

  val _sub = new DynamicVariable[MockDownstream](null)
  def sub = _sub.value

  val op = Operation.FanOutBox[Char, FanOut.Tee](FanOut.Tee, producer ⇒ _sub.value = producer.asInstanceOf[MockDownstream])

  "`Tee` should" - {

    "immediately execute its argument function" in new Test(op) {
      sub should not be null
    }

    "request from upstream only when both substreams have requested" in new Test(op) {
      requestMore(5)
      expectNoRequestMore()
      sub.requestMore(3)
      expectRequestMore(3)
      sub.requestMore(4)
      expectRequestMore(2)
    }

    "propagate elements to both downstreams" in new Test(op) {
      requestMore(2)
      sub.requestMore(2)
      expectRequestMore(2)
      onNext('a')
      expectNext('a')
      sub.expectNext('a')
      onNext('b')
      expectNext('b')
      sub.expectNext('b')
    }

    "propagate completion to both downstreams" in new Test(op) {
      onComplete()
      expectComplete()
      sub.expectComplete()
    }

    "propagate errors to both downstreams" in new Test(op) {
      onError(TestException)
      expectError(TestException)
      sub.expectError(TestException)
    }

    "when main downstream has cancelled" - {

      "propagate requestMores from downstream2" in new Test(op) {
        cancel()
        sub.requestMore(3)
        expectRequestMore(3)
      }

      "'unlock' pending requestMores from downstream2" in new Test(op) {
        sub.requestMore(3)
        expectNoRequestMore()
        cancel()
        expectRequestMore(3)
      }

      "propagate elements to downstreams2" in new Test(op) {
        cancel()
        sub.requestMore(2)
        expectRequestMore(2)
        onNext('a')
        sub.expectNext('a')
        onNext('b')
        sub.expectNext('b')
      }

      "propagate completion to downstream2" in new Test(op) {
        cancel()
        onComplete()
        sub.expectComplete()
      }

      "propagate errors to downstream2" in new Test(op) {
        cancel()
        onError(TestException)
        sub.expectError(TestException)
      }

      "cancel upstream when downstream2 cancels as well" in new Test(op) {
        cancel()
        expectNoCancel()
        sub.cancel()
        expectCancel()
      }
    }

    "when downstream2 has cancelled" - {

      "propagate requestMores from main downstream" in new Test(op) {
        sub.cancel()
        requestMore(3)
        expectRequestMore(3)
      }

      "'unlock' pending requestMores from main downstream" in new Test(op) {
        requestMore(3)
        expectNoRequestMore()
        sub.cancel()
        expectRequestMore(3)
      }

      "propagate elements to main downstreams" in new Test(op) {
        sub.cancel()
        requestMore(2)
        expectRequestMore(2)
        onNext('a')
        expectNext('a')
        onNext('b')
        expectNext('b')
      }

      "propagate completion to main downstream" in new Test(op) {
        sub.cancel()
        onComplete()
        expectComplete()
      }

      "propagate errors to main downstream" in new Test(op) {
        sub.cancel()
        onError(TestException)
        expectError(TestException)
      }

      "cancel upstream when main downstream cancels as well" in new Test(op) {
        sub.cancel()
        expectNoCancel()
        cancel()
        expectCancel()
      }
    }
  }
}