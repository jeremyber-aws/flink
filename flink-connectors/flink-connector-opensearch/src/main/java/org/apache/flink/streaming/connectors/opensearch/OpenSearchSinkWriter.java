package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

/** */
public class OpenSearchSinkWriter<InputT> extends AsyncSinkWriter<InputT, String> {

    private final RestHighLevelClient client;
    private final String indexName;
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSinkWriter.class);

    public OpenSearchSinkWriter(
            ElementConverter<InputT, String> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            int maxBatchSizeInBytes,
            int maxTimeInBufferMS,
            int maxRecordSizeInBytes,
            String osUrl,
            Properties openSearchClientProperties) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.client = buildRestClient(osUrl, openSearchClientProperties);
        this.indexName =
                Optional.ofNullable(
                                openSearchClientProperties.getProperty(
                                        OpenSearchConfigConstants.INDEX_NAME))
                        .orElse(getDefaultIndexName());
    }

    private String getDefaultIndexName() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(LocalDate.now());
    }

    private RestHighLevelClient buildRestClient(
            String osUrl, Properties openSearchClientProperties) {
        RestHighLevelClient client;
        // if username, password is available in properties
        if (openSearchClientProperties.containsKey(
                        OpenSearchConfigConstants.BASIC_CREDENTIALS_USERNAME)
                && openSearchClientProperties.contains(
                        OpenSearchConfigConstants.BASIC_CREDENTIALS_PASSWORD)) {
            String userName =
                    openSearchClientProperties.getProperty(
                            OpenSearchConfigConstants.BASIC_CREDENTIALS_USERNAME);
            String password =
                    openSearchClientProperties.getProperty(
                            OpenSearchConfigConstants.BASIC_CREDENTIALS_PASSWORD);
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
    protected void submitRequestEntries(
            List<String> requestEntries, Consumer<Collection<String>> requestResult) {
        LOG.info("Total requestEntries :{}", requestEntries.size());
        BulkRequest bulkRequest = Requests.bulkRequest();
        for (String element : requestEntries) {
            bulkRequest.add(new IndexRequest(indexName).source(element, XContentType.JSON));
        }
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                LOG.error(bulkResponse.buildFailureMessage());
            }
            for (BulkItemResponse a : bulkResponse.getItems()) {
                LOG.trace("doc id ingested {} ", a.getId());
            }
            LOG.info(
                    "Bulk index of {} documents took {}",
                    requestEntries.size(),
                    bulkResponse.getIngestTookInMillis());
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        // commit the messages as all of them processed.
        // TODO: look for failure messages in bulkResponse.
        requestResult.accept(Collections.emptyList());
    }

    @Override
    protected long getSizeInBytes(String requestEntry) {
        return requestEntry.length();
    }
}
