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

import scala.language.implicitConversions

import scala.concurrent.{ Promise, ExecutionContext, Future }
import scala.collection.immutable.VectorBuilder
import scala.collection.immutable
import org.reactivestreams.api.{ Consumer, Producer }
import akka.actor.ActorRefFactory
import waves.impl.OperationProcessor

sealed trait Flow[+A] {
  def ~>[B](other: A ==> B): Flow[B]
}

object Flow {
  val Empty: Flow[Nothing] = apply(StreamProducer.empty[Nothing])

  def apply[T](producer: Producer[T]): Flow[T] = Unmapped(producer)
  def apply[T](future: Future[T])(implicit ec: ExecutionContext): Flow[T] = Unmapped(StreamProducer(future))
  def apply[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Flow[T] = Unmapped(StreamProducer(iterable))
  def apply[T](iterator: Iterator[T])(implicit ec: ExecutionContext): Flow[T] = Unmapped(StreamProducer(iterator))
  def of[T](elements: T*)(implicit ec: ExecutionContext): Flow[T] = apply(elements)

  implicit def fromProducer[T](producer: Producer[T])(implicit ec: ExecutionContext): Flow[T] = apply(producer)
  implicit def fromFuture[T](future: Future[T])(implicit ec: ExecutionContext): Flow[T] = apply(future)
  implicit def fromIterable[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Flow[T] = apply(iterable)

  implicit class Api[A](val flow: Flow[A]) extends OperationApi[A] {
    type Res[B] = Flow[B]

    def append[B](next: A ==> B): Flow[B] = flow ~> next

    def toProducer(implicit refFactory: ActorRefFactory): Producer[A] =
      flow match {
        case Mapped(producer, op) ⇒
          val processor = new OperationProcessor(op)
          producer.produceTo(processor)
          processor.asInstanceOf[Producer[A]]
        case Unmapped(producer) ⇒ producer
      }

    def produceTo(consumer: Consumer[A])(implicit refFactory: ActorRefFactory): Unit =
      toProducer.produceTo(consumer)

    // returns a future on the first stream element
    def headFuture(implicit refFactory: ActorRefFactory, ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]()
      produceTo(StreamConsumer.headFuture(promise))
      promise.future
    }

    // drains the stream into the given callback
    def drain(callback: A ⇒ Unit)(implicit refFactory: ActorRefFactory, ec: ExecutionContext): Unit =
      onElement(callback) produceTo StreamConsumer.blackHole[A]

    // drains the stream into a Seq
    def drainToSeq(implicit refFactory: ActorRefFactory, ec: ExecutionContext): Future[immutable.Seq[A]] =
      fold(new VectorBuilder[A])(_ += _).map(_.result()).headFuture
  }

  /////////////////////// MODEL //////////////////////

  final case class Mapped[A, B](producer: Producer[A], op: Operation[A, B]) extends Flow[B] {
    def ~>[C](op2: B ==> C): Flow[C] = Mapped(producer, op ~> op2)
  }

  final case class Unmapped[A](producer: Producer[A]) extends Flow[A] {
    def ~>[B](op: A ==> B): Flow[B] = Mapped(producer, op)
  }
}