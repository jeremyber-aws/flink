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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/** */
public class AmazonOpenSearchSinkWriter<InputT> extends AsyncSinkWriter<InputT, String> {

    private final RestHighLevelClient client;
    private final String indexName;

    public AmazonOpenSearchSinkWriter(
            ElementConverter<InputT, String> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String osUrl,
            String userName,
            String password,
            String indexName) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.indexName = indexName;

        System.out.println("initializing connection");
        // Establish credentials to use basic authentication.
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(osUrl)).setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider)
                ));

    }

    @Override
    protected void submitRequestEntries(
            List<String> requestEntries, Consumer<Collection<String>> requestResult) {
        System.out.println("Total requestEntries " + requestEntries.size());
        System.out.println("created hashmap, creating new index / bulk requests");
        BulkRequest bulkRequest = Requests.bulkRequest();
        for (String element : requestEntries) {
            bulkRequest.add(new IndexRequest(indexName).source(element, XContentType.JSON));
        }
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            for (BulkItemResponse a : bulkResponse.getItems()) {
                System.out.println("doc id ingested" + a.getId() + " " + a.getId());
            }
            System.out.println(
                    "Bulk index of "
                            + requestEntries.size()
                            + " documents took "
                            + bulkResponse.getIngestTookInMillis());
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        //commit the messages as all of them processed.
        //TODO: look for failure messages in bulkResponse.
        requestResult.accept(Collections.emptyList());
    }

    @Override
    protected long getSizeInBytes(String requestEntry) {
        return requestEntry.length();
    }
}
