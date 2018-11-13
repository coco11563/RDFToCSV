package datastructure.Util;

import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

public class ThreadUtils {
    public static <T> Future<T> intoFuture(ExecutorService pool, Callable<T> callable) {
        try {
            return pool.submit(() -> {
                return callable.call();
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }

    public final static ExecutorService DEFAULT = createDefaultPool();

    public static ExecutorService createDefaultPool() {
        int threads = Runtime.getRuntime().availableProcessors()*2;
        int queueSize = threads * 25;
        return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());
//                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                // block caller for 100ns
                LockSupport.parkNanos(100);
                try {
                    // submit again
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
