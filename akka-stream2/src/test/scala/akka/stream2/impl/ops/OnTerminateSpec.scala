package akka.stream2.impl.ops

import akka.stream2.Operation

class OnTerminateSpec extends OperationImplSpec {

  "`OnTerminate` should" - {

    "propagate complete and notify the callback" in {
      var callbackCall: Option[Throwable] = null
      val op = Operation.OnTerminate[Int](callbackCall = _)
      new Test(op) {
        onComplete()
        expectComplete()
      }
      callbackCall shouldEqual None
    }

    "propagate onError and notify the callback" in {
      var callbackCall: Option[Throwable] = null
      val op = Operation.OnTerminate[Int](callbackCall = _)
      new Test(op) {
        onError(TestException)
        expectError(TestException)
      }
      callbackCall shouldEqual Some(TestException)
    }
  }
}
