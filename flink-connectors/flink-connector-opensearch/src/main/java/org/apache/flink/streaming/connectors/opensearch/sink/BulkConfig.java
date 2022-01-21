package org.apache.flink.streaming.connectors.opensearch.sink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** OpenSearch Bulk configuration. */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BulkConfig implements Serializable {
    private int bulkMaxSize;
    private int bulkMaxSizeInMb;
    private long bulkFlushInterval;
    private FlushBackoffType flushBackoffType;
    private int bulkBackoffMaxRetries;
    private long bulkBackOffDelay;
}
