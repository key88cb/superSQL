package edu.zju.supersql.regionserver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;

class MiniSqlProcessTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRestartAfterUnexpectedCrash() throws Exception {
        Path script = createFakeMiniSqlScript();
        MiniSqlProcess process = new MiniSqlProcess(script.toString(), tempDir.toString());

        try {
            process.start();
            Assertions.assertTrue(process.isAlive());

            // Trigger a crash in the child process.
            process.execute("crash;");
        } catch (Exception ignored) {
            // Expected: the child exits before producing a prompt.
        }

        waitUntil(Duration.ofSeconds(5), process::isAlive);
        waitUntil(Duration.ofSeconds(5), () -> {
            try {
                return startupCount() >= 2;
            } catch (Exception e) {
                return false;
            }
        });

        String result = process.execute("select * from t;");
        Assertions.assertTrue(result.contains(">>> SUCCESS"));

        process.stop();
    }

    @Test
    void stopShouldDisableAutoRestart() throws Exception {
        Path script = createFakeMiniSqlScript();
        MiniSqlProcess process = new MiniSqlProcess(script.toString(), tempDir.toString());

        process.start();
        Assertions.assertTrue(process.isAlive());
        process.stop();

        Thread.sleep(1_500);
        Assertions.assertFalse(process.isAlive());
        Assertions.assertEquals(1, startupCount());
    }

    private Path createFakeMiniSqlScript() throws Exception {
        Path script = tempDir.resolve("fake-minisql.sh");
        String content = """
                #!/bin/sh
                mkdir -p "$MINISQL_DATA_DIR"
                echo start >> "$MINISQL_DATA_DIR/start_count.log"
                echo ">>> Welcome to MiniSQL"
                while IFS= read -r line
                do
                  if [ "$line" = "exit;" ]; then
                    echo ">>> Bye bye~"
                    exit 0
                  fi
                  if [ "$line" = "crash;" ]; then
                    exit 1
                  fi
                  if [ "$line" = "checkpoint;" ]; then
                    echo ">>> Checkpoint SUCCESS. Data flushed up to LSN: 123"
                    echo ">>> "
                    continue
                  fi
                  echo ">>> SUCCESS"
                  echo ">>> "
                done
                """;
        Files.writeString(script, content);
        script.toFile().setExecutable(true);
        return script;
    }

    private int startupCount() throws Exception {
        Path countFile = tempDir.resolve("start_count.log");
        if (!Files.exists(countFile)) {
            return 0;
        }
        List<String> lines = Files.readAllLines(countFile);
        return lines.size();
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        Assertions.fail("Condition not met within " + timeout.toMillis() + " ms");
    }
}
