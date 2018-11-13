package datastructure.Util

import java.util.concurrent._
import java.util.concurrent.locks.LockSupport

object ThreadUtils {
  def intoFuture[T](pool: ExecutorService , callable: Callable[T]): Future[T] = {
    pool.submit(() => {
      def foo(): T = callable.call
      foo()
    })
  }
  def createDefaultPool :ThreadPoolExecutor = {
    val threads = Runtime.getRuntime.availableProcessors * 2
    val queueSize = threads * 25
    new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](queueSize), new CallerBlocksPolicy)
  }

  class CallerBlocksPolicy() extends RejectedExecutionHandler {
    override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
      if (!executor.isShutdown) { // block caller for 100ns
        LockSupport.parkNanos(100)
        try // submit again
        executor.submit(r).get
        catch {
          case e@(_: InterruptedException | _: ExecutionException) =>
            throw new RuntimeException(e)
        }
      }
    }
  }
}
