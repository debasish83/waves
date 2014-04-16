package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import akka.actor.ActorSystem
import org.scalatest.{ Suite, BeforeAndAfterAll }

trait WithActorSystemScalatest extends BeforeAndAfterAll { this: Suite ⇒
  implicit val system: ActorSystem = ActorSystem()

  override protected def afterAll(): Unit = system.shutdown()
}