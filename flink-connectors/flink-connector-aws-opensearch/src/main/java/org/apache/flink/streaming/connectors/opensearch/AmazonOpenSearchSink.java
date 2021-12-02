package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;


public class AmazonOpenSearchSink<InputT> extends AsyncSinkBase<String, String> {

    private final String indexName;
    private final String hostname;
    private final int port;
    private final String scheme;
    private static final ElementConverter<String, String> ELEMENT_CONVERTER = ((element, context) -> element);
    private static final int MAX_BATCH_SIZE = 1;
    private static final int MAX_IN_FLIGHT_REQUESTS = 1;
    private static final int MAX_BUFFERERED_REQUESTS = 1;
    private static final int MAX_BATCH_SIZE_IN_BYTES = 100_000_000;
    private static final int MAX_TIME_IN_BUFFER_MS = 200;
    private static final int MAX_RECORD_SIZE_IN_BYTES = 100_000;


    protected AmazonOpenSearchSink(
            String indexName,
            String hostname,
            int port,
            String scheme) {
        super(
                ELEMENT_CONVERTER,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES);
        this.indexName = indexName;
        this.hostname = hostname;
        this.port = port;
        this.scheme = scheme;
    }

    @Override
    public SinkWriter<String, Void, Collection<String>> createWriter(
            InitContext context,
            List<Collection<String>> states) throws IOException {
        return new AmazonOpenSearchSinkWriter(ELEMENT_CONVERTER, context,
                MAX_BATCH_SIZE, MAX_IN_FLIGHT_REQUESTS,
                MAX_IN_FLIGHT_REQUESTS, MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS, MAX_RECORD_SIZE_IN_BYTES,
                hostname, port, scheme, indexName);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<String>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
