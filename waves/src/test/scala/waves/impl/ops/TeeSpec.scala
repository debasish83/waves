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

package waves
package impl
package ops

import org.specs2.matcher.Scope

class TeeSpec extends OperationImplSpec {

  "`Tee`" should {

    "immediately execute its argument function" in teeTest { fixture ⇒
      import fixture._
      sub should not be null
    }

    "request from upstream only when both substreams have requested" in teeTest { fixture ⇒
      import fixture._
      requestMore(5)
      expectNoRequestMore()
      sub.requestMore(3)
      expectRequestMore(3)
      sub.requestMore(4)
      expectRequestMore(2)
    }

    "propagate elements to both downstreams" in teeTest { fixture ⇒
      import fixture._
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

    "propagate completion to both downstreams" in teeTest { fixture ⇒
      import fixture._
      onComplete()
      expectComplete()
      sub.expectComplete()
    }

    "propagate errors to both downstreams" in teeTest { fixture ⇒
      import fixture._
      onError(TestException)
      expectError(TestException)
      sub.expectError(TestException)
    }

    "when main downstream has cancelled" >> {

      "propagate requestMores from downstream2" in teeTest { fixture ⇒
        import fixture._
        fixture.cancel()
        sub.requestMore(3)
        expectRequestMore(3)
      }

      "'unlock' pending requestMores from downstream2" in teeTest { fixture ⇒
        import fixture._
        sub.requestMore(3)
        expectNoRequestMore()
        fixture.cancel()
        expectRequestMore(3)
      }

      "propagate elements to downstreams2" in teeTest { fixture ⇒
        import fixture._
        fixture.cancel()
        sub.requestMore(2)
        expectRequestMore(2)
        onNext('a')
        sub.expectNext('a')
        onNext('b')
        sub.expectNext('b')
      }

      "propagate completion to downstream2" in teeTest { fixture ⇒
        import fixture._
        fixture.cancel()
        onComplete()
        sub.expectComplete()
      }

      "propagate errors to downstream2" in teeTest { fixture ⇒
        import fixture._
        fixture.cancel()
        onError(TestException)
        sub.expectError(TestException)
      }

      "cancel upstream when downstream2 cancels as well" in teeTest { fixture ⇒
        import fixture._
        fixture.cancel()
        expectNoCancel()
        sub.cancel()
        expectCancel()
      }
    }

    "when downstream2 has cancelled" >> {

      "propagate requestMores from main downstream" in teeTest { fixture ⇒
        import fixture._
        sub.cancel()
        requestMore(3)
        expectRequestMore(3)
      }

      "'unlock' pending requestMores from main downstream" in teeTest { fixture ⇒
        import fixture._
        requestMore(3)
        expectNoRequestMore()
        sub.cancel()
        expectRequestMore(3)
      }

      "propagate elements to main downstreams" in teeTest { fixture ⇒
        import fixture._
        sub.cancel()
        requestMore(2)
        expectRequestMore(2)
        onNext('a')
        expectNext('a')
        onNext('b')
        expectNext('b')
      }

      "propagate completion to main downstream" in teeTest { fixture ⇒
        import fixture._
        sub.cancel()
        onComplete()
        expectComplete()
      }

      "propagate errors to main downstream" in teeTest { fixture ⇒
        import fixture._
        sub.cancel()
        onError(TestException)
        expectError(TestException)
      }

      "cancel upstream when main downstream cancels as well" in teeTest { fixture ⇒
        import fixture._
        sub.cancel()
        expectNoCancel()
        fixture.cancel()
        expectCancel()
      }
    }
  }

  def teeTest(body: TeeFixture ⇒ Unit) = test(new TeeFixture, body)

  class TeeFixture extends Fixture[Char, Char] with Scope {
    var sub: MockDownstream = _
    def operation = Operation.FanOutBox[Char, FanOut.Tee](FanOut.Tee, p ⇒ sub = p.asInstanceOf[MockDownstream])
  }
}