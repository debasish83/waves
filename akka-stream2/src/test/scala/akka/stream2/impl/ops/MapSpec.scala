package akka.stream2.impl.ops

import akka.stream2.Operation

class MapSpec extends OperationImplSpec {

  val op = Operation.Map[Int, Int](_ * 2)

  "`Map` should" - {
    "propagate requestMore" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(5)
      requestMore(2)
      expectRequestMore(2)
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

    "map the `onNext` element with the user function" in test(op) { fixture ⇒
      import fixture._
      onNext(3)
      expectNext(6)
      onNext(-42)
      expectNext(-84)
    }

    "when user function throws an error: propagate as onError and cancel upstream" in
      test(Operation.Map[Int, Int](_ ⇒ throw TestException)) { fixture ⇒
        import fixture._
        onNext(3)
        expectError(TestException)
        expectCancel()
      }
  }
}
