package org.reactivestreams.tck // TODO: move back out of this package once the visibility problems have been fixed

import org.scalatest.{ Matchers, WordSpecLike }
import akka.actor.ActorSystem
import akka.stream2.AbstractStrictProducer

// same as some of the IdentityProcessorTest cases but directly on the fanout logic level
class AbstractProducerSpec(override val system: ActorSystem) extends TestEnvironment(Timeouts.defaultTimeoutMillis(system))
  with WordSpecLike with Matchers with WithActorSystemScalatest {

  def this() = this(ActorSystem())
  import system.dispatcher

  "An AbstractProducer" should {

    "trigger `requestFromUpstream` for elements that have been requested 'long ago'" in new Test(iSize = 1, mSize = 1) {
      val sub1 = newSubscriber()
      sub1.requestMore(5)

      nextRequestMore() shouldEqual 1
      sendNext('a)
      sub1.nextElement() shouldEqual 'a

      nextRequestMore() shouldEqual 1
      sendNext('b)
      sub1.nextElement() shouldEqual 'b

      nextRequestMore() shouldEqual 1
      val sub2 = newSubscriber()

      // sub1 now has 3 pending
      // sub2 has 0 pending

      sendNext('c)
      sub1.nextElement() shouldEqual 'c
      sub2.expectNone()

      sub2.requestMore(1)
      sub2.nextElement() shouldEqual 'c

      nextRequestMore() shouldEqual 1 // because sub1 still has 2 pending

      verifyNoAsyncErrors()
    }

    "unblock the stream if a 'blocking' subscription has been cancelled" in new Test(iSize = 1, mSize = 1) {
      val sub1 = newSubscriber()
      val sub2 = newSubscriber()

      sub1.requestMore(5)
      nextRequestMore() shouldEqual 1
      sendNext('a)

      expectNoRequestMore() // because we only have buffer size 1 and sub2 hasn't seen 'a yet
      sub2.cancel() // must "unblock"
      nextRequestMore() shouldEqual 1

      verifyNoAsyncErrors()
    }
  }

  class Test(iSize: Int, mSize: Int) extends AbstractStrictProducer[Symbol](iSize, mSize) {
    private val requests = new TestEnvironment.Receptacle[Int](AbstractProducerSpec.this)
    @volatile private var shutDown = false
    protected def pushNext(count: Int): Unit = requests.add(count)
    override protected def shutdown(): Unit = {
      shutDown = true
      super.shutdown()
    }

    def newSubscriber() = newManualSubscriber(this)
    def nextRequestMore(timeoutMillis: Int = 100): Int =
      requests.next(timeoutMillis, "Did not receive expected `requestMore` call")
    def expectNoRequestMore(timeoutMillis: Int = 100): Unit =
      requests.expectNone(timeoutMillis, "Received an unexpected call: requestMore ")
    def sendNext(element: Symbol): Unit = pushToDownstream(element)
    def assertShutDown(): Unit = shutDown shouldEqual true
  }
}