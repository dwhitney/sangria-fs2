package sangria.streaming

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

import _root_.fs2.{Stream, Strategy, Task}

object fs2 {

  class FS2SubscriptionStream(implicit strategy: Strategy, executionContext: ExecutionContext) extends SubscriptionStream[Stream[Task, ?]] {

    def failed[T](e: Throwable): Stream[Task,T] = 
      throw e

    def first[T](s: Stream[Task,T]): Future[T] = 
      s.take(1).runLast.unsafeRunAsyncFuture.map(_.get).recoverWith {
        case e: NoSuchElementException ⇒ Future.failed(new IllegalStateException("Promise was not completed - stream hasn't produced any elements."))
        case e ⇒ Future.failed(e)
      }

    def flatMapFuture[Ctx, Res, T](future: Future[T])(resultFn: T ⇒ Stream[Task,Res]): Stream[Task,Res] =
      Stream.eval[Task, T](Task.fromFuture(future)).flatMap(resultFn)

    def map[A, B](source: Stream[Task,A])(fn: A ⇒ B): Stream[Task,B] = 
      source.map(fn)

    def mapFuture[A, B](source: Stream[Task,A])(fn: A ⇒ Future[B]): Stream[Task,B] =
      source.evalMap(a ⇒ Task.fromFuture(fn(a)))

    def merge[T](streams: Vector[Stream[Task,T]]): Stream[Task,T] = 
      if(streams.isEmpty) throw new IllegalStateException("No streams produced!")
      else streams.foldLeft(Stream.empty[Task,T])(_.merge(_))

    def onComplete[Ctx, Res](result: Stream[Task,Res])(op: ⇒ Unit): Stream[Task,Res] =
      result.onFinalize(Task.delay(op))

    def recover[T](stream: Stream[Task,T])(fn: Throwable ⇒ T): Stream[Task,T] =
      stream.onError(e ⇒ Stream.emit(fn(e)))

    def single[T](value: T): Stream[Task,T] = 
      Stream.apply[Task, T](value)

    def singleFuture[T](value: Future[T]): Stream[Task,T] = 
      Stream.eval[Task,T](Task.fromFuture(value))

    def supported[T[_]](other: SubscriptionStream[T]): Boolean = 
      other.isInstanceOf[FS2SubscriptionStream]

  }

  implicit def fs2SubscriptionStream(implicit strategy: Strategy, executionContext: ExecutionContext): SubscriptionStream[Stream[Task, ?]] =
      new FS2SubscriptionStream

}