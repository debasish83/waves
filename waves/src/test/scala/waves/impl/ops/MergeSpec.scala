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

class MergeSpec extends OperationImplSpec {

  val producer2 = mockProducer[Char]
  val op = Operation.Merge[Symbol, Any](producer2)

  "`Merge`" should {

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
      def playToSubscribedWithOneRequested[A, B](fixture: Fixture[A, B]) = {
        import fixture._
        val upstream2 = expectRequestSecondaryUpstream(producer2)
        requestMore(1)
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

      "when one element is requested" >> {
        "propagate cancels to both upstreams" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          fixture.cancel()
          expectCancel()
          upstream2.expectCancel()
        }

        "immediately dispatch a primary element and buffer a subsequent secondary" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          onNext('a)
          expectNext('a)
          upstream2.onNext('b')
          expectNoNext()
          requestMore(2)
          expectNext('b')
          expectRequestMore(1)
          upstream2.expectRequestMore(1)
        }
        "immediately dispatch a secondary element and buffer a subsequent primary" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          upstream2.onNext('a')
          expectNext('a')
          onNext('b)
          expectNoNext()
          requestMore(2)
          expectNext('b)
          expectRequestMore(1)
          upstream2.expectRequestMore(1)
        }

        "buffer primary completion and complete to downstream upon secondary completion" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          onComplete()
          expectNoComplete()
          upstream2.onComplete()
          expectComplete()
        }
        "buffer secondary completion and complete to downstream upon primary completion" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          upstream2.onComplete()
          expectNoComplete()
          onComplete()
          expectComplete()
        }

        "immediately error downstream and cancel secondary upstream when primary upstream errors" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          onError(TestException)
          expectError(TestException)
          upstream2.expectCancel()
        }
        "immediately error downstream and cancel primary upstream when secondary upstream errors" in test(op) { fixture ⇒
          val upstream2 = playToSubscribedWithOneRequested(fixture)
          import fixture._
          upstream2.onError(TestException)
          expectError(TestException)
          expectCancel()
        }
      }
    }
  }
}
