package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/** */
public class OpenSearchSink<InputT> extends AsyncSinkBase<InputT, String> {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSink.class);

    private final String osUrl;
    private Properties openSearchClientProperties;

    public OpenSearchSink(
            ElementConverter<InputT, String> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            String osUrl,
            Properties openSearchClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.osUrl =
                Preconditions.checkNotNull(
                        osUrl,
                        "The OpenSearch host url name must not be null when initializing the OpenSearch Sink.");
        Preconditions.checkArgument(
                !this.osUrl.isEmpty(),
                "The OpenSearch host url name must not be null when initializing the OpenSearch Sink.");
        this.openSearchClientProperties = openSearchClientProperties;
    }

    /**
     * Create a {@link OpenSearchSinkBuilder} to allow the fluent construction of a new {@code
     * OpenSearchSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link OpenSearchSinkBuilder}
     */
    public static <InputT> OpenSearchSinkBuilder<InputT> builder() {
        return new OpenSearchSinkBuilder<>();
    }

    @Override
    public SinkWriter<InputT, Void, Collection<String>> createWriter(
            InitContext context, List<Collection<String>> states) {
        LOG.info("creating writer...");
        return new OpenSearchSinkWriter(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                osUrl,
                openSearchClientProperties);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<String>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
