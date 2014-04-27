/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.stream2

import org.reactivestreams.api.{ Consumer, Producer }
import org.reactivestreams.spi.Publisher

private[stream2] trait AbstractProducer[T] extends Producer[T] with Publisher[T] {
  def getPublisher: Publisher[T] = this
  def produceTo(consumer: Consumer[T]) = subscribe(consumer.getSubscriber)
}
