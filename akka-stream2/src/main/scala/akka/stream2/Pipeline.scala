package akka.stream2

import org.reactivestreams.api.{ Consumer, Producer }

case class Pipeline(producer: Producer[Any], operation: OperationX, consumer: Consumer[Nothing])

object Pipeline {
  def untyped(producer: Producer[_], operation: OperationX, consumer: Consumer[_]): Pipeline =
    apply(producer.asInstanceOf[Producer[Any]], operation, consumer.asInstanceOf[Consumer[Nothing]])
}