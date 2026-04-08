package edu.zju.supersql.testutil;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public final class EmbeddedZkServerFactory {

    private EmbeddedZkServerFactory() {
    }

    public static TestingServer create() throws Exception {
        File dataDir = Files.createTempDirectory("zk-test-").toFile();
        Map<String, Object> props = new HashMap<>();
        props.put("maxCnxns", "60");
        props.put("maxClientCnxns", "60");
        InstanceSpec spec = new InstanceSpec(dataDir, -1, -1, -1, true, -1, -1, 60, props);
        return new TestingServer(spec, true);
    }
}
