package org.apache.flink.streaming.connectors.opensearch.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.opensearch.OpenSearchConfigConstants;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * OpenSearchWriter that writes/flushes the sink by calling the emitter.
 *
 * @param <InputT>
 */
public class OpenSearchWriter<InputT> implements SinkWriter<InputT, Void, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchWriter.class);

    private final OpenSearchEmitter<? super InputT> emitter;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;
    private final RequestIndexer requestIndexer;
    private final Counter numBytesOutCounter;

    private long pendingActions = 0;
    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0;
    private volatile long ackTime = Long.MAX_VALUE;
    private volatile boolean closed = false;

    /**
     * Constructor creating an OpenSearch writer.
     *
     * @param osUrl the reachable OpenSearch url
     * @param emitter converting incoming records to OpenSearch actions
     * @param flushOnCheckpoint if true all until now received records are flushed after every
     *     checkpoint
     * @param bulkConfig describing the flushing and failure handling of the used {@link
     *     BulkProcessor}
     * @param openSearchClientProperties describing properties of the network connection used to
     *     connect to the OpenSearch cluster
     * @param mailboxExecutor Flink's mailbox executor
     */
    OpenSearchWriter(
            OpenSearchEmitter<? super InputT> emitter,
            boolean flushOnCheckpoint,
            BulkConfig bulkConfig,
            String osUrl,
            Properties openSearchClientProperties,
            SinkWriterMetricGroup metricGroup,
            MailboxExecutor mailboxExecutor) {
        this.emitter = checkNotNull(emitter);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.client = buildRestClient(osUrl, openSearchClientProperties);
        this.bulkProcessor = createBulkProcessor(bulkConfig);
        this.requestIndexer = new DefaultRequestIndexer();
        checkNotNull(metricGroup);
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        try {
            emitter.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the OpenSearchEmitter", e);
        }
    }

    private BulkProcessor createBulkProcessor(BulkConfig bulkConfig) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        new BiConsumer<BulkRequest, ActionListener<BulkResponse>>() {
                            @Override
                            public void accept(
                                    BulkRequest bulkRequest,
                                    ActionListener<BulkResponse> bulkResponseActionListener) {
                                client.bulkAsync(
                                        bulkRequest,
                                        RequestOptions.DEFAULT,
                                        bulkResponseActionListener);
                            }
                        },
                        new BulkProcessor.Listener() {
                            @Override
                            public void beforeBulk(long l, BulkRequest bulkRequest) {
                                LOG.info(
                                        "Sending bulk of {} actions to OpenSearch.",
                                        bulkRequest.numberOfActions());
                                lastSendTime = System.currentTimeMillis();
                                numBytesOutCounter.inc(bulkRequest.estimatedSizeInBytes());
                            }

                            @Override
                            public void afterBulk(
                                    long executionId,
                                    BulkRequest bulkRequest,
                                    BulkResponse bulkResponse) {
                                ackTime = System.currentTimeMillis();
                                enqueueActionInMailbox(
                                        () -> extractFailures(bulkRequest, bulkResponse),
                                        "OpenSearchSuccessCallback");
                            }

                            @Override
                            public void afterBulk(
                                    long executionId,
                                    BulkRequest bulkRequest,
                                    Throwable throwable) {
                                enqueueActionInMailbox(
                                        () -> {
                                            throw new FlinkRuntimeException(
                                                    "Complete bulk has failed.", throwable);
                                        },
                                        "OpenSearchErrorCallback");
                            }
                        });
        if (bulkConfig.getBulkMaxSize() != -1) {
            builder.setBulkActions(bulkConfig.getBulkMaxSize());
        }

        if (bulkConfig.getBulkMaxSizeInMb() != -1) {
            builder.setBulkSize(
                    new ByteSizeValue(bulkConfig.getBulkMaxSizeInMb(), ByteSizeUnit.MB));
        }

        if (bulkConfig.getBulkFlushInterval() != -1) {
            builder.setFlushInterval(new TimeValue(bulkConfig.getBulkFlushInterval()));
        }

        BackoffPolicy backoffPolicy;
        final TimeValue backoffDelay = new TimeValue(bulkConfig.getBulkBackOffDelay());
        final int maxRetryCount = bulkConfig.getBulkBackoffMaxRetries();
        switch (bulkConfig.getFlushBackoffType()) {
            case CONSTANT:
                backoffPolicy = BackoffPolicy.constantBackoff(backoffDelay, maxRetryCount);
                break;
            case EXPONENTIAL:
                backoffPolicy = BackoffPolicy.exponentialBackoff(backoffDelay, maxRetryCount);
                break;
            case NONE:
                backoffPolicy = BackoffPolicy.noBackoff();
                break;
            default:
                throw new IllegalArgumentException(
                        "Received unknown backoff policy type " + bulkConfig.getFlushBackoffType());
        }
        builder.setBackoffPolicy(backoffPolicy);
        return builder.build();
    }

    private RestHighLevelClient buildRestClient(
            String osUrl, Properties openSearchClientProperties) {
        RestHighLevelClient client;
        String userName =
                openSearchClientProperties.getProperty(
                        OpenSearchConfigConstants.BASIC_CREDENTIALS_USERNAME);
        String password =
                openSearchClientProperties.getProperty(
                        OpenSearchConfigConstants.BASIC_CREDENTIALS_PASSWORD);
        // if username, password is available in properties
        if (!StringUtils.isNullOrWhitespaceOnly(userName)
                && !StringUtils.isNullOrWhitespaceOnly(password)) {
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            client =
                    new RestHighLevelClient(
                            RestClient.builder(HttpHost.create(osUrl))
                                    .setHttpClientConfigCallback(
                                            httpClientBuilder ->
                                                    httpClientBuilder.setDefaultCredentialsProvider(
                                                            credentialsProvider)));
            // TODO: Other auth implementations (sigv4, etc..)
        } else {
            client = new RestHighLevelClient(RestClient.builder(HttpHost.create(osUrl)));
        }
        return client;
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        emitter.emit(element, context, requestIndexer);
    }

    @Override
    public List prepareCommit(boolean flush) throws IOException, InterruptedException {
        checkpointInProgress = true;
        while (pendingActions != 0 && (flushOnCheckpoint || flush)) {
            bulkProcessor.flush();
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
        checkpointInProgress = false;
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        emitter.close();
        bulkProcessor.close();
        client.close();
    }

    private class DefaultRequestIndexer implements RequestIndexer {

        @Override
        public void add(IndexRequest... indexRequests) {
            for (final IndexRequest indexRequest : indexRequests) {
                pendingActions++;
                bulkProcessor.add(indexRequest);
            }
        }

        @Override
        public void add(UpdateRequest... updateRequests) {
            for (final UpdateRequest updateRequest : updateRequests) {
                pendingActions++;
                bulkProcessor.add(updateRequest);
            }
        }

        @Override
        public void add(DeleteRequest... deleteRequests) {
            for (final DeleteRequest deleteRequest : deleteRequests) {
                pendingActions++;
                bulkProcessor.add(deleteRequest);
            }
        }
    }

    private void enqueueActionInMailbox(
            ThrowingRunnable<? extends Exception> action, String actionName) {
        // If the writer is cancelled before the last bulk response (i.e. no flush on checkpoint
        // configured or shutdown without a final
        // checkpoint) the mailbox might already be shutdown, so we should not enqueue any
        // actions.
        if (isClosed()) {
            return;
        }
        mailboxExecutor.execute(action, actionName);
    }

    private void extractFailures(BulkRequest request, BulkResponse response) {
        if (!response.hasFailures()) {
            pendingActions -= request.numberOfActions();
            return;
        }

        Throwable chainedFailures = null;
        for (int i = 0; i < response.getItems().length; i++) {
            final BulkItemResponse itemResponse = response.getItems()[i];
            if (!itemResponse.isFailed()) {
                continue;
            }
            final Throwable failure = itemResponse.getFailure().getCause();
            if (failure == null) {
                continue;
            }
            final RestStatus restStatus = itemResponse.getFailure().getStatus();
            final DocWriteRequest<?> actionRequest = request.requests().get(i);

            chainedFailures =
                    firstOrSuppressed(
                            wrapException(restStatus, failure, actionRequest), chainedFailures);
        }
        if (chainedFailures == null) {
            return;
        }
        throw new FlinkRuntimeException(chainedFailures);
    }

    private boolean isClosed() {
        if (closed) {
            LOG.warn("Writer was closed before all records were acknowledged by OpenSearch.");
        }
        return closed;
    }

    private static Throwable wrapException(
            RestStatus restStatus, Throwable rootFailure, DocWriteRequest<?> actionRequest) {
        if (restStatus == null) {
            return new FlinkRuntimeException(
                    String.format("Single action %s of bulk request failed.", actionRequest),
                    rootFailure);
        } else {
            return new FlinkRuntimeException(
                    String.format(
                            "Single action %s of bulk request failed with status %s.",
                            actionRequest, restStatus.getStatus()),
                    rootFailure);
        }
    }
}
