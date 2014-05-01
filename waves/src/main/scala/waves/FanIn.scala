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

import scala.language.higherKinds

import waves.impl._

trait FanIn[I1, I2, O] {
  def requestMore(elements: Int): Unit
  def cancel(): Unit

  def primaryOnNext(element: I1): Unit
  def primaryOnComplete(): Unit
  def primaryOnError(cause: Throwable): Unit
  def secondaryOnNext(element: I2): Unit
  def secondaryOnComplete(): Unit
  def secondaryOnError(cause: Throwable): Unit
}

object FanIn {
  trait Provider[I1, I2, O] {
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): FanIn[I1, I2, O]
  }

  object Merge extends Provider[Any, Any, Any] {
    def apply[A, AA >: A](): Provider[A, AA, AA] = this.asInstanceOf[Provider[A, AA, AA]]
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Merge[Any] =
      new Merge(primaryUpstream, secondaryUpstream, downstream)
  }

  class Merge[T](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[T, T, T] {
    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: T): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: T): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }

  object MergeToEither extends Provider[Any, Any, Either[Any, Any]] {
    def apply[A, B](): Provider[A, B, Either[A, B]] = this.asInstanceOf[Provider[A, B, Either[A, B]]]
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): MergeToEither[Any, Any] =
      new MergeToEither(primaryUpstream, secondaryUpstream, downstream)
  }

  class MergeToEither[A, B](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[A, B, Either[A, B]] {
    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: A): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: B): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }

  object Zip extends Provider[Any, Any, (Any, Any)] {
    def apply[A, B](): Provider[A, B, (A, B)] = this.asInstanceOf[Provider[A, B, (A, B)]]
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Zip[Any, Any] =
      new Zip(primaryUpstream, secondaryUpstream, downstream)
  }

  class Zip[A, B](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[A, B, (A, B)] {
    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: A): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: B): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }
}