package org.apache.flink.streaming.connectors.opensearch.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Used to control whether the sink should retry failed requests at all or with which kind back off
 * strategy.
 */
@PublicEvolving
public enum FlushBackoffType {
    /** After every failure, it waits a configured time until the retries are exhausted. */
    CONSTANT,
    /**
     * After every failure, it waits initially the configured time and increases the waiting time
     * exponentially until the retries are exhausted.
     */
    EXPONENTIAL,
    /** The failure is not retried. */
    NONE,
}
