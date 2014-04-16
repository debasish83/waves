package akka.stream2.impl.ops

import akka.stream2.Operation
import Operation.Process._

class ProcessSpec extends OperationImplSpec with MultiplyTests {

  "`Process` should allow for high-level operation implementations like" - {

    "Fold[Char, String](\"\", _ + _)" - {
      val op = Operation.Process[Char, String, String](
        seed = "",
        onNext = (acc, c) ⇒ Continue(acc + c),
        onComplete = acc ⇒ Emit(acc, Stop))

      "fold upstream elements with the user function" in new Test(op) {
        requestMore(1)
        expectRequestMore(1)
        onNext('A')
        expectRequestMore(1)
        onNext('B')
        expectRequestMore(1)
        onNext('C')
        expectRequestMore(1)
        onComplete()
        expectNext("ABC")
        expectComplete()
      }
    }

    "Multiply(5)" - {
      multiply5Tests {
        Operation.Process[Char, Char, Unit](
          seed = (),
          onNext = (_, c) ⇒ Emit(c, Emit(c, Emit(c, Emit(c, Emit(c, Continue(())))))),
          onComplete = _ ⇒ Stop)
      }
    }

    "Take(3)" - {
      val op = Operation.Process[Char, Char, Int](
        seed = 3,
        onNext = (remaining, c) ⇒ Emit(c, if (remaining > 1) Continue(remaining - 1) else Stop),
        onComplete = _ ⇒ Stop)

      "propagate the first n elements from upstream, then complete downstream and cancel upstream" in new Test(op) {
        requestMore(2)
        expectRequestMore(1)
        onNext('A')
        expectNext('A')
        expectRequestMore(1)
        onNext('B')
        expectNext('B')
        expectNoRequestMore()
        requestMore(3)
        expectRequestMore(1)
        onNext('C')
        expectNext('C')
        expectComplete()
        expectCancel()
      }
    }
  }

  "`Process` should" - {
    "for an op that throws from its `onNext` function" - {
      val op = Operation.Process[Char, Char, Unit](
        seed = (),
        onNext = (_, _) ⇒ throw TestException,
        onComplete = _ ⇒ Stop)

      "propagate as onError and cancel upstream" in new Test(op) {
        requestMore(3)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
    }

    "for an op that throws from its `onComplete` function" - {
      val op = Operation.Process[Char, Char, Int](
        seed = 3,
        onNext = (remaining, c) ⇒ Emit(c, if (remaining > 1) Continue(remaining - 1) else Stop),
        onComplete = _ ⇒ throw TestException)

      "propagate as onError and cancel upstream" in new Test(op) {
        requestMore(2)
        expectRequestMore(1)
        onNext('A')
        expectNext('A')
        expectRequestMore(1)
        onComplete()
        expectError(TestException)
        expectCancel()
      }
    }
  }
}
