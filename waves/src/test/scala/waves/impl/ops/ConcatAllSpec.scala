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

package waves.impl.ops

import org.reactivestreams.api.Producer
import waves.Operation

class ConcatAllSpec extends OperationImplSpec {

  val op = Operation.ConcatAll[Producer[Char], Char]

  "`Flatten`" should {

    "request one from super stream on first requestMore from downstream" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(1)
      requestMore(2)
      expectNoRequestMore()
    }

    "while waiting for sub-stream subscription" >> {
      def playToWaitingForSubstreamSubscription[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        val flowA = mockProducer
        onNext(flowA)
        expectRequestSubUpstream(flowA)
      }

      "gather up requestMore calls from downstream" in test(op) { fixture ⇒
        val subUpstream = playToWaitingForSubstreamSubscription(fixture)
        import fixture._
        requestMore(2, 3)
        subUpstream.onSubscribe()
        subUpstream.expectRequestMore(6)
      }

      "properly handle empty sub streams" in test(op) { fixture ⇒
        val subUpstream = playToWaitingForSubstreamSubscription(fixture)
        import fixture._
        subUpstream.onComplete()
        expectRequestMore(1)
      }

      "when receiving an error from the sub-stream" >> {
        "immediately propagate error to downstream and cancel super upstream if not yet cancelled" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          subUpstream.onError(TestException)
          expectError(TestException)
          expectCancel()
        }
        "ignore error if downstream already cancelled" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          fixture.cancel()
          expectCancel()
          subUpstream.onError(TestException)
          expectNoError()
          expectNoCancel()
        }
      }

      "when cancelled from downstream" >> {
        "immediately cancel super stream and eventually cancel non-empty sub stream" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          fixture.cancel()
          expectCancel()
          subUpstream.onSubscribe()
          subUpstream.expectCancel()
        }
        "immediately cancel super stream and ignore empty sub stream" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          fixture.cancel()
          expectCancel()
          subUpstream.onComplete()
        }
      }

      "remember onComplete from super stream and complete after" >> {
        "non-empty sub stream completion" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
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
        "empty sub stream completion" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          onComplete()
          subUpstream.onComplete()
          expectComplete()
        }
      }

      "remember onError from super stream and propagate after" >> {
        "non-empty sub stream completion" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          onError(TestException)
          subUpstream.onSubscribe()
          subUpstream.expectRequestMore(1)
          subUpstream.onNext('A')
          expectNext('A')
          expectNoError()
          subUpstream.onComplete()
          expectError(TestException)
        }
        "empty sub stream completion" in test(op) { fixture ⇒
          val subUpstream = playToWaitingForSubstreamSubscription(fixture)
          import fixture._
          onError(TestException)
          subUpstream.onComplete()
          expectError(TestException)
        }
      }
    }

    "while consuming substream" >> {
      def playToConsumingSubstream[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        val flowA = mockProducer
        onNext(flowA)
        val subUpstream = expectRequestSubUpstream(flowA)
        subUpstream.onSubscribe()
        subUpstream.expectRequestMore(1)
        subUpstream
      }

      "request elements from sub stream and produce to downstream" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
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

      "request next from super stream when sub stream is completed and properly carry over requested count" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.expectNoRequestMore()
        requestMore(5)
        subUpstream.expectRequestMore(5)
        subUpstream.onComplete()
        expectRequestMore(1)
        val flowB = mockProducer
        onNext(flowB)
        val subUpstream2 = expectRequestSubUpstream(flowB)
        subUpstream2.onSubscribe()
        subUpstream2.expectRequestMore(5)
      }

      "continue to produce to downstream when super stream completes and complete upon sub stream completion" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
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

      "continue to produce to downstream when super stream errors and propagate upon sub stream completion" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
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

      "cancel sub stream and main stream when cancelled from downstream" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
        subUpstream.onNext('A')
        expectNext('A')
        fixture.cancel()
        subUpstream.expectCancel()
        expectCancel()
      }

      "immediately propagate sub stream error and cancel super stream" in test(op) { fixture ⇒
        val subUpstream = playToConsumingSubstream(fixture)
        import fixture._
        subUpstream.onNext('A')
        expectNext('A')
        subUpstream.onError(TestException)
        expectError(TestException)
        expectCancel()
      }
    }
  }
}
