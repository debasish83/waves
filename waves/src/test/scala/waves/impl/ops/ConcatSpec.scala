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

import waves.Operation

class ConcatSpec extends OperationImplSpec {

  val producer2 = mockProducer[Char]
  val op = Operation.Concat[Char](() ⇒ producer2)

  "`Concat`" should {

    "before completion of first upstream" >> {

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

      "not propagate complete" in test(op) { fixture ⇒
        import fixture._
        onComplete()
        expectNoComplete()
        expectRequestSecondaryUpstream(producer2)
      }

      "propagate error" in test(op) { fixture ⇒
        import fixture._
        onError(TestException)
        expectError(TestException)
      }

      "forward elements to downstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(1, 2)
        expectRequestMore(1, 2)
        onNext('A', 'B', 'C')
        expectNext('A', 'B', 'C')
      }
    }

    "while waiting for subscription to second upstream" >> {
      def playToWaitingForSubscription[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onComplete()
        expectRequestSecondaryUpstream(producer2)
      }

      "gather up requestMore calls from downstream" in test(op) { fixture ⇒
        val upstream2 = playToWaitingForSubscription(fixture)
        import fixture._
        requestMore(2)
        requestMore(1) // now the downstream has requested a total of 4 elements
        upstream2.onSubscribe()
        upstream2.expectRequestMore(4)
      }

      "immediately complete downstream if second upstream is completed before subscription" in test(op) { fixture ⇒
        val upstream2 = playToWaitingForSubscription(fixture)
        import fixture._
        upstream2.onComplete()
        expectComplete()
      }

      "when receiving an error from the second upstream" >> {
        "immediately propagate error to downstream if downstream has not yet cancelled" in test(op) { fixture ⇒
          val upstream2 = playToWaitingForSubscription(fixture)
          import fixture._
          upstream2.onError(TestException)
          expectError(TestException)
        }
        "ignore error if downstream already cancelled" in test(op) { fixture ⇒
          val upstream2 = playToWaitingForSubscription(fixture)
          import fixture._
          fixture.cancel()
          upstream2.onError(TestException)
          expectNoError()
        }
      }

      "when cancelled from downstream" >> {
        "eventually cancel second upstream" in test(op) { fixture ⇒
          val upstream2 = playToWaitingForSubscription(fixture)
          import fixture._
          fixture.cancel()
          upstream2.onSubscribe()
          upstream2.expectCancel()
        }
        "don't cancel second upstream if it is empty" in test(op) { fixture ⇒
          val upstream2 = playToWaitingForSubscription(fixture)
          import fixture._
          fixture.cancel()
          upstream2.onComplete()
          upstream2.expectNoCancel()
        }
      }
    }

    "after subscription to second upstream" >> {
      def playToAfterSubscriptionToSecondUpstream[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onComplete()
        val upstream2 = expectRequestSecondaryUpstream(producer2)
        upstream2.onSubscribe()
        upstream2.expectRequestMore(1)
        upstream2
      }

      "propagate requestMore" in test(op) { fixture ⇒
        val upstream2 = playToAfterSubscriptionToSecondUpstream(fixture)
        import fixture._
        requestMore(5)
        upstream2.expectRequestMore(5)
        requestMore(2)
        upstream2.expectRequestMore(2)
      }

      "propagate cancel" in test(op) { fixture ⇒
        val upstream2 = playToAfterSubscriptionToSecondUpstream(fixture)
        import fixture._
        fixture.cancel()
        upstream2.expectCancel()
      }

      "propagate complete" in test(op) { fixture ⇒
        val upstream2 = playToAfterSubscriptionToSecondUpstream(fixture)
        import fixture._
        upstream2.onComplete()
        expectComplete()
      }

      "propagate error" in test(op) { fixture ⇒
        val upstream2 = playToAfterSubscriptionToSecondUpstream(fixture)
        import fixture._
        upstream2.onError(TestException)
        expectError(TestException)
      }

      "forward elements to downstream" in test(op) { fixture ⇒
        val upstream2 = playToAfterSubscriptionToSecondUpstream(fixture)
        import fixture._
        requestMore(3)
        upstream2.expectRequestMore(3)
        upstream2.onNext('A', 'B', 'C')
        expectNext('A', 'B', 'C')
      }
    }
  }
}
