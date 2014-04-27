package akka.stream2.impl.ops

import akka.stream2.Operation

class BufferSpec extends OperationImplSpec {

  "`Buffer` should" - {

    "for an op that converts incoming Symbols into Chars and keeps a bounded buffer modelled as a String" - {
      val op = Operation.Buffer[Symbol, Char, String](
        seed = "ABC", // initially the buffer has this state
        compress = _ + _.name.charAt(0), // append symbols incoming from upstream to the end of buffer string
        expand = {
          case "" ⇒ "" -> None // if the buffer is empty we cannot produce
          case s  ⇒ s.substring(1) -> Some(s.charAt(0)) // otherwise dispatch the oldest char in the buffer
        },
        canConsume = _.length < 5) // buffer is bounded to a capacity of 5

      "drain the seed state if no elements come in from upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(2)
        expectRequestMore(1)
        expectNext('A', 'B')
        expectNoNext()
        requestMore(3)
        expectNext('C')
      }

      "consume from upstream until buffer is full if no further elements are requested from downstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        expectNext('A') // buffer is now "BC"
        onNext('D) // buffer is now "BCD"
        expectRequestMore(1)
        onNext('E) // buffer is now "BCDE"
        expectRequestMore(1)
        onNext('F) // buffer is now "BCDEF", i.e. full
        expectNoRequestMore()
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

    "for an op that converts incoming Symbols into Chars and caches the last result for immediate re-dispatch to downstream" - {
      val op = Operation.Buffer[Symbol, Char, Option[Char]]( // consumes from upstream at max rate, produces to downstream at max rate
        seed = None,
        compress = (buffer, element) ⇒ Some(element.name.charAt(0)),
        expand = buffer ⇒ buffer -> buffer,
        canConsume = _ ⇒ true)

      "when downstream consumes faster than upstream can produce" - {
        "produce the last element at max rate to downstream and update when new element comes in from upstream" in test(op) { fixture ⇒
          import fixture._
          requestMore(3)
          expectRequestMore(1)
          onNext('A)
          expectRequestMore(1)
          expectNext('A', 'A', 'A')
          requestMore(2)
          expectNext('A', 'A')
          onNext('B)
          expectRequestMore(1)
          requestMore(2)
          expectNext('B', 'B')
        }
      }

      "when upstream produces faster than downstream can consume" - {
        "compress elements coming in from upstream and dispatch the current result when downstream requests" in test(op) { fixture ⇒
          import fixture._
          requestMore(1)
          expectRequestMore(1)
          onNext('A)
          expectRequestMore(1)
          expectNext('A')
          onNext('B)
          expectRequestMore(1)
          onNext('C)
          expectRequestMore(1)
          requestMore(1)
          expectNext('C')
          onNext('D)
          expectRequestMore(1)
        }
      }
    }

    "for an op that throws from its `compress` function" - {
      val op = Operation.Buffer[Symbol, Char, Option[Char]](
        seed = None,
        compress = (_, _) ⇒ throw TestException,
        expand = buffer ⇒ buffer -> buffer,
        canConsume = _ ⇒ true)

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(3)
        expectRequestMore(1)
        onNext('A)
        expectError(TestException)
        expectCancel()
      }
    }

    "for an op that throws from its `expand` function" - {
      val op = Operation.Buffer[Symbol, Char, Option[Char]](
        seed = None,
        compress = (buffer, element) ⇒ Some(element.name.charAt(0)),
        expand = _ ⇒ throw TestException,
        canConsume = _ ⇒ true)

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(3)
        expectError(TestException)
        expectCancel()
      }
    }

    "for an op that throws from its `canConsume` function" - {
      val op = Operation.Buffer[Symbol, Char, Option[Char]](
        seed = None,
        compress = (buffer, element) ⇒ Some(element.name.charAt(0)),
        expand = buffer ⇒ buffer -> buffer,
        canConsume = _ ⇒ throw TestException)

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(3)
        expectError(TestException)
        expectCancel()
      }
    }
  }
}
