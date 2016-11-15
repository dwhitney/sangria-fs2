package sangria.streaming

import java.util.concurrent.atomic.AtomicInteger

import language.postfixOps
import org.scalatest.{Matchers, WordSpec}

import _root_.fs2.{Stream, Strategy, Task}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class FS2IntegrationSpec extends WordSpec with Matchers {
  implicit val strategy: Strategy = Strategy.fromExecutionContext(
    scala.concurrent.ExecutionContext.Implicits.global)

  val impl: SubscriptionStream[Stream[Task, ?]] = new sangria.streaming.fs2.FS2SubscriptionStream

  "fs2 Integration" should {
    "support itself" in {
      impl.supported(sangria.streaming.fs2.fs2SubscriptionStream) should be (true)
    }

    "map" in {
      res(impl.map(Stream.emits[Task, Int](List(1, 2, 10)))(_ + 1)) should be (Vector(2, 3, 11))
    }

    "singleFuture" in {
      res(impl.singleFuture(Future.successful("foo"))) should be (List("foo"))
    }

    "single" in {
      res(impl.single("foo")) should be (List("foo"))
    }

    "mapFuture" in {
      res(impl.mapFuture(Stream.emits[Task, Int](List(1, 2, 10)))(x ⇒ Future.successful(x + 1))) should be (List(2, 3, 11))
    }

    "first" in {
      res(impl.first(Stream.emits[Task, Int](List(1, 2, 3)))) should be (1)
    }

    "first throws error on empty" in {
      an [IllegalStateException] should be thrownBy res(impl.first(Stream.empty[Task, Int]))
    }

    "failed" in {
      an [IllegalStateException] should be thrownBy res(impl.failed(new IllegalStateException("foo")))
    }

    "onComplete handles success" in {
      val stream = Stream.empty[Task, Int]
      val count = new AtomicInteger(0)
      def inc() = count.getAndIncrement()

      val updated = impl.onComplete(stream)(inc())

      Await.ready(updated.runLog.unsafeRunAsyncFuture, 2 seconds)

      count.get() should be (1)
    }

    "onComplete handles failure" in {
      val stream = Stream.fail[Task](new IllegalStateException("foo"))
      val count = new AtomicInteger(0)
      def inc() = count.getAndIncrement()

      val updated = impl.onComplete(stream)(inc())

      Await.ready(updated.runLog.unsafeRunAsyncFuture, 2 seconds)

      count.get() should be (1)
    }

    "flatMapFuture" in {
      res(impl.flatMapFuture(Future.successful(1))(i ⇒ Stream.emits[Task, String](List(i.toString, (i + 1).toString)))) should be (Vector("1", "2"))
    }

    "recover" in {
      val stream = Stream.emits[Task, Int](List(1, 2, 3, 4)) map { i ⇒
        if (i == 3) throw new IllegalStateException("foo")
        else i
      }

      println(stream.onError(_ ⇒ Stream.emit(100)).runLog.unsafeRunSync)

      res(impl.recover(stream)(_ ⇒ 100)) should be (Vector(1, 2, 100))
    }

    "merge" in {
      val stream1 = Stream.emits[Task, Int](List(1, 2))
      val stream2 = Stream.emits[Task, Int](List(3, 4))
      val stream3 = Stream.emits[Task, Int](List(100, 200))

      val result = res(impl.merge(Vector(stream1, stream2, stream3)))

      result should (
        have(size(6)) and
        contain(1) and
        contain(2) and
        contain(3) and
        contain(4) and
        contain(100) and
        contain(200))
    }

    "merge 2" in {
      val stream1 = Stream.emits[Task, Int](List(1, 2))
      val stream2 = Stream.emits[Task, Int](List(100, 200))

      val result = res(impl.merge(Vector(stream1, stream2)))

      result should (
        have(size(4)) and
        contain(1) and
        contain(2) and
        contain(100) and
        contain(200))
    }

    "merge 1" in {
      val stream = Stream.emits[Task, Int](List(1, 2))

      val result = res(impl.merge(Vector(stream)))

      result should (
        have(size(2)) and
        contain(1) and
        contain(2))
    }

    "merge throws exception on empty" in {
      an [IllegalStateException] should be thrownBy impl.merge(Vector.empty)
    }
  }

  def res[T](stream: Stream[Task, T]) =
    Await.result(stream.runLog.unsafeRunAsyncFuture, 2 seconds)

  def res[T](f: Future[T]) =
    Await.result(f, 2 seconds)
}