package org.apache.flink.streaming.connectors.opensearch.sink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.sink.SinkWriter;

import org.opensearch.action.ActionRequest;

/**
 * OpenSearch Emitter to generate an {@link ActionRequest}.
 *
 * @param <T>
 */
public interface OpenSearchEmitter<T> extends Function {

    /**
     * Initialization method for the function. It is called once before the actual working process
     * methods.
     */
    default void open() throws Exception {}

    /** Tear-down method for the function. It is called when the sink closes. */
    default void close() throws Exception {}

    /**
     * Process the incoming element to produce multiple {@link ActionRequest ActionRequests}. The
     * produced requests should be added to the provided {@link RequestIndexer}.
     *
     * @param element incoming element to process
     * @param context to access additional information about the record
     * @param indexer request indexer that {@code ActionRequest} should be added to
     */
    void emit(T element, SinkWriter.Context context, RequestIndexer indexer);
}
