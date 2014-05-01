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

class TransformSpec extends OperationImplSpec with MultiplyTests {

  "`Transform` should allow for high-level operation implementations like" >> {

    "Fold[Char, String](\"\", _ + _)" >> {
      val op = Operation.Transform {
        new Operation.Transformer[Char, String] {
          val sb = new java.lang.StringBuilder
          def onNext(c: Char) = { sb.append(c); Nil }
          override def onComplete = sb.toString :: Nil
        }
      }

      "fold upstream elements with the user function" in test(op) { fixture ⇒
        import fixture._
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

    "Multiply(5)" >> {
      multiply5Tests {
        Operation.Transform {
          new Operation.Transformer[Char, Char] {
            def onNext(c: Char) = Seq.fill(5)(c)
          }
        }
      }
    }

    "Take(3)" >> {
      val op = Operation.Transform {
        new Operation.Transformer[Char, Char] {
          var remaining = 3
          def onNext(c: Char) = { remaining -= 1; c :: Nil }
          override def isComplete = remaining == 0
        }
      }

      "propagate the first n elements from upstream, then complete downstream and cancel upstream" in test(op) { fixture ⇒
        import fixture._
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

  "`Transform`" should {
    "for an op that throws from its `onNext` function" >> {
      val op = Operation.Transform {
        new Operation.Transformer[Char, Char] {
          def onNext(c: Char) = throw TestException
        }
      }

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(3)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
    }

    "for an op that throws from its `isComplete` function" >> {
      val op = Operation.Transform {
        new Operation.Transformer[Char, Char] {
          var remaining = 3
          def onNext(c: Char) = { remaining -= 1; c :: Nil }
          override def isComplete = throw TestException
        }
      }

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(2)
        expectRequestMore(1)
        onNext('A')
        expectNext('A')
        expectError(TestException)
        expectCancel()
      }
    }

    "for an op that throws from its `onComplete` function" >> {
      val op = Operation.Transform {
        new Operation.Transformer[Char, Char] {
          var remaining = 3
          def onNext(c: Char) = { remaining -= 1; c :: Nil }
          override def onComplete = throw TestException
        }
      }

      "propagate as onError and cancel upstream" in test(op) { fixture ⇒
        import fixture._
        requestMore(2)
        expectRequestMore(1)
        onNext('A')
        expectNext('A')
        expectRequestMore(1)
        onComplete()
        expectError(TestException)
      }
    }
  }
}
