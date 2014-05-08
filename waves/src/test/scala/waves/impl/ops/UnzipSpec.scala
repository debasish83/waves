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

class UnzipSpec extends OperationImplSpec {

  "`Unzip`" should {

    "immediately execute its argument function" in unzipTest { fixture ⇒
      import fixture._
      sub should not be null
    }

    "request from upstream only when both substreams have requested" in unzipTest { fixture ⇒
      import fixture._
      requestMore(5)
      expectNoRequestMore()
      sub.requestMore(3)
      expectRequestMore(3)
      sub.requestMore(4)
      expectRequestMore(2)
    }

    "split incoming tuples to both downstreams" in unzipTest { fixture ⇒
      import fixture._
      requestMore(2)
      sub.requestMore(2)
      expectRequestMore(2)
      onNext('a -> 'b')
      expectNext('a)
      sub.expectNext('b')
      onNext('c -> 'd')
      expectNext('c)
      sub.expectNext('d')
    }

    "propagate completion to both downstreams" in unzipTest { fixture ⇒
      import fixture._
      onComplete()
      expectComplete()
      sub.expectComplete()
    }

    "propagate errors to both downstreams" in unzipTest { fixture ⇒
      import fixture._
      onError(TestException)
      expectError(TestException)
      sub.expectError(TestException)
    }

    "cancel upstream when primary downstream cancels" in unzipTest { fixture ⇒
      import fixture._
      fixture.cancel()
      expectCancel()
    }

    "cancel upstream when secondary downstream cancels" in unzipTest { fixture ⇒
      import fixture._
      sub.cancel()
      expectCancel()
    }
  }

  def unzipTest(body: UnzipFixture ⇒ Unit) = test(new UnzipFixture, body)

  class UnzipFixture extends Fixture[(Symbol, Char), Symbol] with Scope {
    var sub: MockDownstream = _
    def operation = Operation.Unzip[Symbol, Char](p ⇒ sub = p.asInstanceOf[MockDownstream])
  }
}