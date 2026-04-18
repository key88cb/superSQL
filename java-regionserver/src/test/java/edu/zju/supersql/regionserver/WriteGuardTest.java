package edu.zju.supersql.regionserver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WriteGuardTest {

    @Test
    void pauseAndResumeCorrectly() {
        WriteGuard guard = new WriteGuard();

        Assertions.assertFalse(guard.isPaused("orders"));
        guard.pause("orders");
        Assertions.assertTrue(guard.isPaused("orders"));
        guard.resume("orders");
        Assertions.assertFalse(guard.isPaused("orders"));
    }

    @Test
    void pauseOneTableDoesNotAffectAnother() {
        WriteGuard guard = new WriteGuard();
        guard.pause("table_a");
        Assertions.assertFalse(guard.isPaused("table_b"));
    }

    @Test
    void awaitWritableReturnsWhenResumed() throws InterruptedException {
        WriteGuard guard = new WriteGuard();
        guard.pause("t");

        Thread resumeThread = new Thread(() -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            guard.resume("t");
        });
        resumeThread.setDaemon(true);
        resumeThread.start();

        long start = System.currentTimeMillis();
        guard.awaitWritable("t", 2_000);
        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertFalse(guard.isPaused("t"));
        // Should have waited roughly 100 ms, not the full 2000 ms
        Assertions.assertTrue(elapsed < 1_000, "awaitWritable took too long: " + elapsed + "ms");
    }
}
