package org.apache.flink.streaming.connectors.opensearch.sink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** OpenSearch Bulk configuration. */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BulkConfig {
    private int bulkFlushMaxActions;
    private int bulkFlushMaxMb;
    private long bulkFlushInterval;
    private FlushBackoffType flushBackoffType;
    private int bulkFlushBackoffRetries;
    private long bulkFlushBackOffDelay;
}
