package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import java.util.Optional;
import java.util.Properties;

/**
 * OpenSearch Sink Builder.
 * @param <InputT>
 */
public class OpenSearchSinkBuilder<InputT> extends AsyncSinkBaseBuilder<
        InputT, String, OpenSearchSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 5000;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 10000; // must be > max_batch_size
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 100;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 10000000;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 1000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 10000000;

    private String osUrl;
    private Properties openSearchClientProperties;

    OpenSearchSinkBuilder(){}

    /**
     * Sets the OpenSearch domain URL that the sink will connect to. There is no default for this
     * parameter, therefore, this must be provided at sink creation time otherwise the build will
     * fail.
     *
     * @param osUrl the name of the stream
     * @return {@link OpenSearchSinkBuilder} itself
     */
    public OpenSearchSinkBuilder<InputT> setOpenSearchHost(String osUrl) {
        this.osUrl = osUrl;
        return this;
    }

    public OpenSearchSinkBuilder<InputT> setOpenSearchClientProperties(
            Properties openSearchClientProperties) {
        this.openSearchClientProperties = openSearchClientProperties;
        return this;
    }

    @Override
    public OpenSearchSink<InputT> build() {
        return new OpenSearchSink<>(
                getElementConverter(),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                osUrl,
                openSearchClientProperties
        );
    }
}
