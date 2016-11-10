package sangria.streaming

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

import _root_.fs2.{Stream, Strategy, Task}

object fs2{

  class StreamSubscriptionStream(implicit executionContext: ExecutionContext) extends SubscriptionStream[({type λ[α] = Stream[Task, α]})#λ]{

    implicit val strategy: Strategy = Strategy.fromExecutionContext(executionContext)

    def failed[T](e: Throwable): Stream[Task,T] = 
      throw e

    def first[T](s: Stream[Task,T]): Future[T] = 
      s.take(1).runLast.unsafeRunAsyncFuture.map(_.get)

    def flatMapFuture[Ctx, Res, T](future: Future[T])(resultFn: T => Stream[Task,Res]): Stream[Task,Res] = 
      Stream.eval[Task, T](Task.fromFuture(future)).flatMap(resultFn)

    def map[A, B](source: Stream[Task,A])(fn: A => B): Stream[Task,B] = 
      source.map(fn)

    def mapFuture[A, B](source: Stream[Task,A])(fn: A => Future[B]): Stream[Task,B] = 
      source.evalMap(a => Task.fromFuture(fn(a)))

    def merge[T](streams: Vector[Stream[Task,T]]): Stream[Task,T] = 
      streams.foldLeft(Stream.empty[Task,T])(_.merge(_))

    def onComplete[Ctx, Res](result: Stream[Task,Res])(op: => Unit): Stream[Task,Res] = 
      result.onFinalize(Task.delay(op))

    def recover[T](stream: Stream[Task,T])(fn: Throwable => T): Stream[Task,T] = 
      stream.onError(e => Stream.emit(fn(e)))

    def single[T](value: T): Stream[Task,T] = 
      Stream.apply[Task, T](value)

    def singleFuture[T](value: Future[T]): Stream[Task,T] = 
      Stream.eval[Task,T](Task.fromFuture(value))

    def supported[T[X]](other: SubscriptionStream[T]): Boolean = 
      other.isInstanceOf[StreamSubscriptionStream]

  }

  implicit def streamSubscriptionStream(implicit executionContext: ExecutionContext): SubscriptionStream[({type λ[α] = Stream[Task, α]})#λ] =
      new StreamSubscriptionStream

}