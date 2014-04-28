package akka.stream2.impl.ops

import akka.stream2.Operation

class BufferSpec extends OperationImplSpec {

  val op = Operation.Buffer[Symbol](4)

  "`Buffer` should" - {

    "request and store`size - 1` elements immediately after start" in test(op) { fixture ⇒
      import fixture._
      expectRequestMore(3)
      onNext('a, 'b', 'c)
    }

    "deliver the stored elements to downstream without requesting more from upstream" in test(op) { fixture ⇒
      import fixture._
      expectRequestMore(3)
      onNext('a, 'b, 'c)
      requestMore(1)
      expectNext('a)
      expectNoRequestMore()
      requestMore(4)
      expectNext('b, 'c)
      expectRequestMore(3)
    }

    // TODO: complete
  }
}
