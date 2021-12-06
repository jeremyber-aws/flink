package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class AmazonOpenSearchSinkWriter<InputT> extends AsyncSinkWriter<InputT, String> {

    private final RestHighLevelClient client;
    private String indexName;

    public AmazonOpenSearchSinkWriter(
            ElementConverter<InputT, String> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String hostname,
            int port,
            String scheme,
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
        //Establish credentials to use basic authentication.
        //Only for demo purposes. Do not specify your credentials in code.
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials("jeremy", "Fm3oJMaj!"));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, scheme))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        client = new RestHighLevelClient(builder);

    }

    @Override
    protected void submitRequestEntries(
            List<String> requestEntries,
            Consumer<Collection<String>> requestResult) {
        System.out.println("creating new hashmap");
        HashMap<String, String> stringMapping = new HashMap<String, String>();
        System.out.println("created hashmap, creating new index / bulk requests");
        BulkRequest bulkRequest = new BulkRequest(indexName);
        IndexRequest request = new IndexRequest(indexName);

        for(String element : requestEntries)
        {
            stringMapping.put("message", element);
        }

        request.source(stringMapping);
        bulkRequest.add(request);

        try {
            BulkResponse indexResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
            System.exit(1);
        }
        }

    @Override
    protected long getSizeInBytes(String requestEntry) {
        return requestEntry.length();
    }
}
