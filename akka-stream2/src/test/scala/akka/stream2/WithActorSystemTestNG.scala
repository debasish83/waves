package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import akka.actor.ActorSystem
import org.testng.annotations.AfterClass

trait WithActorSystemTestNG {
  val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  @AfterClass
  def shutdownActorSystem(): Unit = system.shutdown()
}