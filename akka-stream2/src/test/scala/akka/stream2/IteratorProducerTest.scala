package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import org.scalatest.testng.TestNGSuiteLike
import org.reactivestreams.spi.Publisher
import akka.actor.ActorSystem
import akka.stream2.{ IteratorProducer, StreamProducer }

class IteratorProducerTest(_system: ActorSystem, env: TestEnvironment, publisherShutdownTimeout: Long)
  extends PublisherVerification[Int](env, publisherShutdownTimeout)
  with WithActorSystemTestNG with TestNGSuiteLike {

  def this(system: ActorSystem) =
    this(system, new TestEnvironment(Timeouts.defaultTimeoutMillis(system)), Timeouts.publisherShutdownTimeoutMillis)

  def this() = this(ActorSystem())

  override val system = _system
  import system.dispatcher

  def createPublisher(elements: Int): Publisher[Int] = {
    val iter = Iterator from 1000
    IteratorProducer(if (elements > 0) iter take elements else iter).getPublisher
  }

  override def createCompletedStatePublisher(): Publisher[Int] =
    StreamProducer.empty[Int].getPublisher

  def createErrorStatePublisher(): Publisher[Int] = null
}