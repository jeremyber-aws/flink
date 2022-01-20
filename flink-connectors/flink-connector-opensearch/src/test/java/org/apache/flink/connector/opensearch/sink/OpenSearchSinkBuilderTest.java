package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.streaming.connectors.opensearch.sink.OpenSearchSink;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Builder Test. */
public class OpenSearchSinkBuilderTest {

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> OpenSearchSink.builder().openSearchHost("https://dummy").build())
                .withMessageContaining("ElementConverter must be not null");
    }

    @Test
    public void openSearchUrlOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                OpenSearchSink.<String>builder()
                                        .emitter(((element, context, indexer) -> {}))
                                        .build())
                .withMessageContaining(
                        "The OpenSearch host url name must not be null when initializing the OpenSearch Sink.");
    }

    @Test
    public void openSearchUrlOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                OpenSearchSink.<String>builder()
                                        .openSearchHost("")
                                        .emitter(((element, context, indexer) -> {}))
                                        .build())
                .withMessageContaining(
                        "The OpenSearch host url name must not be null when initializing the OpenSearch Sink.");
    }
}
