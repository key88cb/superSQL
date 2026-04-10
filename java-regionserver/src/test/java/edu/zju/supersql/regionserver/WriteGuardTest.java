package edu.zju.supersql.regionserver;

import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import edu.zju.supersql.rpc.ReplicaSyncService;
import edu.zju.supersql.regionserver.rpc.ReplicaSyncServiceImpl;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.TMultiplexedProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
