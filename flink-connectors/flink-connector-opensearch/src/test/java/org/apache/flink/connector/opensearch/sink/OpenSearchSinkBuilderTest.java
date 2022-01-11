package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.streaming.connectors.opensearch.OpenSearchSink;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class OpenSearchSinkBuilderTest {

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> OpenSearchSink
                        .builder()
                        .setOpenSearchHost("https://dummy")
                        .build())
                .withMessageContaining(
                        "ElementConverter must be not null");
    }

    @Test
    public void openSearchUrlOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                OpenSearchSink.<String>builder()
                                        .setElementConverter(((element, context) -> element))
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
                                        .setOpenSearchHost("")
                                        .setElementConverter(((element, context) -> element))
                                        .build())
                .withMessageContaining(
                        "The OpenSearch host url name must not be null when initializing the OpenSearch Sink.");
    }
}
