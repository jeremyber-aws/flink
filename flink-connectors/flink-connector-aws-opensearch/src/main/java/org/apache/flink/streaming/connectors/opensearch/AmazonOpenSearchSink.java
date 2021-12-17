package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** */
public class AmazonOpenSearchSink<InputT> extends AsyncSinkBase<InputT, String> {

    private final String indexName;
    private final String osUrl;
    private final String userName;
    private final String password;

    private static final ElementConverter<Object, String> ELEMENT_CONVERTER =
            ((element, context) -> element.toString());
    private static final int MAX_BATCH_SIZE = 5000;
    private static final int MAX_IN_FLIGHT_REQUESTS = 10000; // must be > max_batch_size
    private static final int MAX_BUFFERED_REQUESTS = 100;
    private static final int MAX_BATCH_SIZE_IN_BYTES = 10000000;
    private static final int MAX_TIME_IN_BUFFER_MS = 1000;
    private static final int MAX_RECORD_SIZE_IN_BYTES = 10000000;

    public AmazonOpenSearchSink(String osUrl, String username, String password, String indexName) {
        super(
                (ElementConverter<InputT, String>) ELEMENT_CONVERTER,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES);
        this.indexName = indexName;
        this.osUrl = osUrl;
        this.userName = username;
        this.password = password;
    }

    @Override
    public SinkWriter<InputT, Void, Collection<String>> createWriter(
            InitContext context, List<Collection<String>> states) {
        System.out.println("creating writer...");
        return new AmazonOpenSearchSinkWriter(
                ELEMENT_CONVERTER,
                context,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                osUrl,
                userName,
                password,
                indexName);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<String>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
