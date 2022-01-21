package org.apache.flink.streaming.connectors.opensearch.sink;

import java.util.Properties;

/**
 * OpenSearchSinkBuilder creates a Sink for OpenSearch.
 *
 * @param <InputT>
 */
public class OpenSearchSinkBuilder<InputT> {
    private int maxBatchSize = 5000;
    private int maxBatchSizeInMb = -1;
    private long bulkFlushInterval = -1;
    private FlushBackoffType bulkFlushBackoffType = FlushBackoffType.NONE;
    private int bulkFlushBackoffRetries = -1;
    private long bulkFlushBackOffDelay = -1;

    private String osUrl;
    private Properties openSearchClientProperties;

    private OpenSearchEmitter<InputT> emitter;

    OpenSearchSinkBuilder() {}

    /**
     * Sets the OpenSearch domain URL that the sink will connect to. There is no default for this
     * parameter, therefore, this must be provided at sink creation time otherwise the build will
     * fail.
     *
     * @param osUrl the name of the stream
     * @return {@link org.apache.flink.streaming.connectors.opensearch.sink.OpenSearchSinkBuilder}
     *     itself
     */
    public OpenSearchSinkBuilder<InputT> openSearchHost(String osUrl) {
        this.osUrl = osUrl;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> openSearchClientProperties(
            Properties openSearchClientProperties) {
        this.openSearchClientProperties = openSearchClientProperties;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> maxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> maxBatchSizeInMb(int maxBatchSizeInMb) {
        this.maxBatchSizeInMb = maxBatchSizeInMb;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> bulkFlushInterval(int bulkFlushInterval) {
        this.bulkFlushInterval = bulkFlushInterval;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> bulkFlushBackoffRetries(int bulkFlushBackoffRetries) {
        this.bulkFlushBackoffRetries = bulkFlushBackoffRetries;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> bulkFlushBackOffDelay(long bulkFlushBackOffDelay) {
        this.bulkFlushBackOffDelay = bulkFlushBackOffDelay;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> bulkFlushBackoffType(
            FlushBackoffType bulkFlushBackoffType) {
        this.bulkFlushBackoffType = bulkFlushBackoffType;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> emitter(OpenSearchEmitter<InputT> emitter) {
        this.emitter = emitter;
        return this;
    }

    public OpenSearchSink<InputT> build() {
        return new OpenSearchSink<>(
                this.osUrl,
                this.openSearchClientProperties,
                BulkConfig.builder()
                        .bulkBackOffDelay(this.bulkFlushBackOffDelay)
                        .bulkBackoffMaxRetries(this.bulkFlushBackoffRetries)
                        .bulkFlushInterval(this.bulkFlushInterval)
                        .bulkMaxSize(this.maxBatchSize)
                        .bulkMaxSizeInMb(this.maxBatchSizeInMb)
                        .flushBackoffType(this.bulkFlushBackoffType)
                        .build(),
                this.emitter);
    }
}
