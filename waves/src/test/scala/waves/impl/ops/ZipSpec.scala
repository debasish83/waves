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

class ZipSpec extends OperationImplSpec {

  val producer2 = mockProducer[Char]
  val op = Operation.Zip[Symbol, Char](producer2)

  "`Zip`" should {

    "immediately subscribe to the secondary upstream" in test(op) { fixture ⇒
      import fixture._
      expectRequestSecondaryUpstream(producer2)
    }

    "while waiting for subscription to the secondary upstream" >> {

      "collect requestMores" in test(op) { fixture ⇒
        import fixture._
        expectRequestSecondaryUpstream(producer2)
        requestMore(5)
        requestMore(2)
        expectNoRequestMore()
      }

      "when cancelled" >> {
        "immediately cancel primary upstream and eventually the secondary upstream when subscribed normally" in test(op) { fixture ⇒
          import fixture._
          val upstream2 = expectRequestSecondaryUpstream(producer2)
          fixture.cancel()
          expectCancel()
          upstream2.onSubscribe()
          upstream2.expectCancel()
        }
        "immediately cancel primary upstream and ignore completion on secondary upstream" in test(op) { fixture ⇒
          import fixture._
          val upstream2 = expectRequestSecondaryUpstream(producer2)
          fixture.cancel()
          expectCancel()
          upstream2.onComplete()
        }
        "immediately cancel primary upstream and ignore error on secondary upstream" in test(op) { fixture ⇒
          import fixture._
          val upstream2 = expectRequestSecondaryUpstream(producer2)
          fixture.cancel()
          expectCancel()
          upstream2.onError(TestException)
        }
      }

      "immediately propagate to downstream and eventually cancel the secondary upstream when completed" in test(op) { fixture ⇒
        import fixture._
        val upstream2 = expectRequestSecondaryUpstream(producer2)
        onComplete()
        expectComplete()
        upstream2.onSubscribe()
        upstream2.expectCancel()
      }

      "immediately propagate to downstream and eventually cancel the secondary upstream when errored" in test(op) { fixture ⇒
        import fixture._
        val upstream2 = expectRequestSecondaryUpstream(producer2)
        onError(TestException)
        expectError(TestException)
        upstream2.onSubscribe()
        upstream2.expectCancel()
      }
    }

    "after subscription to the secondary upstream" >> {
      def playToSubscribed[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        val upstream2 = expectRequestSecondaryUpstream(producer2)
        requestMore(5)
        upstream2.onSubscribe()
        expectRequestMore(1)
        upstream2.expectRequestMore(1)
        upstream2
      }

      "request" >> {
        "one from both upstreams if something has already been requested from downstream" in test(op) { fixture ⇒
          import fixture._
          val upstream2 = expectRequestSecondaryUpstream(producer2)
          requestMore(5)
          upstream2.onSubscribe()
          expectRequestMore(1)
          upstream2.expectRequestMore(1)
        }
        "nothing from either upstream if nothing has already been requested from downstream" in test(op) { fixture ⇒
          import fixture._
          val upstream2 = expectRequestSecondaryUpstream(producer2)
          upstream2.onSubscribe()
          expectNoRequestMore()
        }
      }

      "when no element is buffered" >> {
        "propagate cancels to both upstreams" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          fixture.cancel()
          expectCancel()
          upstream2.expectCancel()
        }
        "buffer an incoming primary element" in test(op) { fixture ⇒
          playToSubscribed(fixture)
          import fixture._
          onNext('a)
        }
        "buffer an incoming secondary element" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          upstream2.onNext('b')
        }
        "immediately complete downstream and cancel secondary upstream when primary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          onComplete()
          expectComplete()
          upstream2.expectCancel()
        }
        "immediately complete downstream and cancel primary upstream when secondary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          upstream2.onComplete()
          expectComplete()
          expectCancel()
        }

        "immediately error downstream and cancel secondary upstream when primary upstream errors" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          onError(TestException)
          expectError(TestException)
          upstream2.expectCancel()
        }
        "immediately error downstream and cancel primary upstream when secondary upstream errors" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          upstream2.onError(TestException)
          expectError(TestException)
          expectCancel()
        }
      }

      "when a primary element is buffered" >> {
        "dispatch tuple to downstream when secondary element arrives" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          onNext('a)
          upstream2.onNext('b')
          expectNext('a -> 'b')
          expectRequestMore(1)
          upstream2.expectRequestMore(1)
        }
        "wait for secondary element before dispatching tuple, completing downstream and cancelling secondary upstream when primary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          onNext('a)
          onComplete()
          expectNoComplete()
          upstream2.expectNoCancel()
          upstream2.onNext('b')
          expectNext('a -> 'b')
          expectComplete()
          upstream2.expectCancel()
        }
        "immediately complete downstream and cancel primary upstream when secondary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          onNext('a)
          upstream2.onComplete()
          expectComplete()
          expectCancel()
        }
      }

      "when a secondary element is buffered" >> {
        "dispatch tuple to downstream when primary element arrives" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          upstream2.onNext('b')
          onNext('a)
          expectNext('a -> 'b')
          expectRequestMore(1)
          upstream2.expectRequestMore(1)
        }
        "immediately complete downstream and cancel secondary upstream when primary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          upstream2.onNext('b')
          onComplete()
          expectComplete()
          upstream2.expectCancel()
        }
        "wait for primary element before dispatching tuple, completing downstream and cancelling primary upstream when secondary upstream completes" in test(op) { fixture ⇒
          val upstream2 = playToSubscribed(fixture)
          import fixture._
          upstream2.onNext('b')
          upstream2.onComplete()
          expectNoComplete()
          expectNoCancel()
          onNext('a)
          expectNext('a -> 'b')
          expectComplete()
          expectCancel()
        }
      }
    }
  }
}
