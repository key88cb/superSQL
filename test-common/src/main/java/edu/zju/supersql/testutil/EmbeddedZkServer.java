package edu.zju.supersql.testutil;

import org.apache.curator.test.TestingServer;

public final class EmbeddedZkServer implements AutoCloseable {

    private final TestingServer delegate;

    EmbeddedZkServer(TestingServer delegate) {
        this.delegate = delegate;
    }

    public String getConnectString() {
        return delegate.getConnectString();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
