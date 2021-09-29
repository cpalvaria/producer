package main.java;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.*;
import com.amazonaws.services.s3.model.S3DataSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {

    static String[] users = Utils.GetUsersFromFile();
    static String[] arns = Utils.GetARNsFromFile();


    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    //timestamp for records
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());

    /**
     * Main method
     * @param args the command line args for the Producer
     * @see com.amazonaws.services.kinesis.producer for positional arg ordering
     */
    public static void main(String[] args) throws Exception {

        final ProducerConfig config = new ProducerConfig(args);

        log.info(String.format("Stream name: %s Region: %s seconds to Run %d", config.getStreamName(), config.getRegion(),
                config.getSecondsToRun()));
        log.info(String.format("Will attempt to run the KPL at %f MB/s...",(config.getDataSize() * config.getRecordsPerSecond())/(1000000.0)));

        final KinesisProducer producer = new KinesisProducer(config.transformToKinesisProducerConfiguration());

        //The increasing sequence number that is put in the data of each record
        final AtomicLong sequenceNumber = new AtomicLong(0);

        //The number of records that have finished (either successfully put, or failed)
        final AtomicLong completed = new AtomicLong(0);

        //KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onSuccess(@Nullable UserRecordResult userRecordResult) {
                completed.getAndIncrement();
                log.info("Successfully put record");
            }

            @Override
            public void onFailure(Throwable t) {
                // If we see any failures, we will log them.
                if (t instanceof UserRecordFailedException) {
                    int attempts = ((UserRecordFailedException) t).getResult().getAttempts().size()-1;
                    Attempt last = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts);
                    if(attempts > 1) {
                        Attempt previous = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts - 1);
                        log.error(String.format(
                                "Record failed to put - %s : %s. Previous failure - %s : %s",
                                last.getErrorCode(), last.getErrorMessage(), previous.getErrorCode(), previous.getErrorMessage()));
                    }else{
                        log.error(String.format(
                                "Record failed to put - %s : %s.",
                                last.getErrorCode(), last.getErrorMessage()));
                    }

                } else if (t instanceof UnexpectedMessageException) {
                    log.error("Record failed to put due to unexpected message received from native layer",
                            t);
                }
                log.error("Exception during put", t);
            }
        };

        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();

        final Runnable putOneRecord = new Runnable() {
            @Override
            public void run() {
//                String[] users = Utils.SetUsers();
//                String[] arns = Utils.SetARNs();

                String ctr = Utils.generateCTR(sequenceNumber.get(), arns, users);


                ByteBuffer data = null;
                try {
                    data = ByteBuffer.wrap(ctr.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                ListenableFuture<UserRecordResult> f = producer.addUserRecord(config.getStreamName(), Utils.randomExplicitHashKey(), data);
                Futures.addCallback(f, callback, callbackThreadPool);
            }
        };

        //This gives us progress updates
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long put = sequenceNumber.get();
                long total = config.getRecordsPerSecond() * config.getSecondsToRun();
                double putPercent = 100.0 * put / total;
                long done = completed.get();
                double donePercent = 100.0 * done / total;
                log.info(String.format(
                        "Put %d of %d so far (%.2f %%), %d have completed (%.2f %%)",
                        put, total, putPercent, done, donePercent));
                log.info(String.format("Oldest future as of now in millis is %s", producer.getOldestRecordTimeInMillis
                        ()));
            }
        }, 1, 1, TimeUnit.SECONDS);

        // Kick off the puts
        log.info(String.format(
                "Starting puts... will run for %d seconds at %d records per second", config.getSecondsToRun(),
                config.getRecordsPerSecond()));
        executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, config.getSecondsToRun(),
                config.getRecordsPerSecond());

        // Wait for puts to finish. After this statement returns, we have
        // finished all calls to putRecord, but the records may still be
        // in-flight. We will additionally wait for all records to actually
        // finish later.
        EXECUTOR.awaitTermination(config.getSecondsToRun() + 1, TimeUnit.SECONDS);

        // If you need to shutdown your application, call flushSync() first to
        // send any buffered records. This method will block until all records
        // have finished (either success or fail). There are also asynchronous
        // flush methods available.
        //
        // Records are also automatically flushed by the KPL after a while based
        // on the time limit set with Configuration.setRecordMaxBufferedTime()
        log.info("Waiting for remaining puts to finish...");
        producer.flushSync();
        log.info("All records complete.");

        // This kills the child process and shuts down the threads managing it.
        producer.destroy();
        log.info("Finished.");
        log.info("Number of files sent: " + sequenceNumber.get());
    }


    /**
     * Executes a function N times per second for M seconds with a
     * ScheduledExecutorService. The executor is shutdown at the end. This is
     * more precise than simply using scheduleAtFixedRate.
     *
     * @param exec
     *            Executor
     * @param task
     *            Task to perform
     * @param counter
     *            Counter used to track how many times the task has been
     *            executed
     * @param durationSeconds
     *            How many seconds to run for
     * @param ratePerSecond
     *            How many times to execute task per second
     */
    private static void executeAtTargetRate(
            final ScheduledExecutorService exec,
            final Runnable task,
            final AtomicLong counter,
            final int durationSeconds,
            final int ratePerSecond) {
        exec.scheduleWithFixedDelay(new Runnable() {
            final long startTime = System.nanoTime();

            @Override
            public void run() {
                double secondsRun = (System.nanoTime() - startTime) / 1e9;
                double targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond;

                while (counter.get() < targetCount) {
                    counter.getAndIncrement();
                    try {
                        task.run();
                    } catch (Exception e) {
                        log.error("Error running task", e);
                        System.exit(1);
                    }
                }

                if (secondsRun >= durationSeconds) {
                    exec.shutdown();
                    log.info("shutting down...");
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }
}
