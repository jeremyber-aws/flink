package org.apache.flink.streaming.connectors.opensearch.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * OpenSearch Sink.
 *
 * @param <InputT>
 */
@AllArgsConstructor
public class OpenSearchSink<InputT> implements Sink<InputT, Void, Void, Void> {

    @NonNull private final String osUrl;
    private final Properties openSearchConnectionProperties;
    @NonNull private final BulkConfig bulkConfig;
    @NonNull private final OpenSearchEmitter emitter;

    @Override
    public SinkWriter<InputT, Void, Void> createWriter(InitContext context, List<Void> states)
            throws IOException {
        return new OpenSearchWriter<>(
                emitter,
                true, // assume true now
                // TODO: replace with DeliveryGuarantee#NONE and DeliveryGuarantee#AT_LEAST_ONCE
                bulkConfig,
                osUrl,
                openSearchConnectionProperties,
                context.metricGroup(),
                context.getMailboxExecutor());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<Committer<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    /**
     * Create a {@link org.apache.flink.streaming.connectors.opensearch.sink.OpenSearchSinkBuilder}
     * to allow the fluent construction of a new {@code OpenSearchSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link org.apache.flink.streaming.connectors.opensearch.sink.OpenSearchSinkBuilder}
     */
    public static <InputT> OpenSearchSinkBuilder<InputT> builder() {
        return new OpenSearchSinkBuilder<>();
    }
}
