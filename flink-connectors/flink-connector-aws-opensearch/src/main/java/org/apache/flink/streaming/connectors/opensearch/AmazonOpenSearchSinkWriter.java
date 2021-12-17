package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

/** */
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
        // Establish credentials to use basic authentication.
        // Only for demo purposes. Do not specify your credentials in code.
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials("admin", "HappyClip#1"));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(hostname, port, scheme))
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder.setDefaultCredentialsProvider(
                                                credentialsProvider));

        client = new RestHighLevelClient(builder);
    }

    @Override
    protected void submitRequestEntries(
            List<String> requestEntries, Consumer<Collection<String>> requestResult) {
        System.out.println("creating new hashmap");
        HashMap<String, String> stringMapping = new HashMap<String, String>();
        System.out.println("created hashmap, creating new index / bulk requests");
        BulkRequest bulkRequest = Requests.bulkRequest(); // new BulkRequest(indexName);
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
            client.close(); // close the client otherwise it leaves too many open files.
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    //    @Override
    //    protected void submitRequestEntries(
    //            List<String> requestEntries,
    //            Consumer<Collection<String>> requestResult) {
    //        System.out.println("creating new hashmap");
    //        HashMap<String, String> stringMapping = new HashMap<String, String>();
    //        System.out.println("created hashmap, creating new index / bulk requests");
    //        BulkRequest bulkRequest = new BulkRequest(indexName);
    //        IndexRequest request = new IndexRequest(indexName);
    //
    //        for(String element : requestEntries)
    //        {
    //            stringMapping.put("message", element);
    //        }
    //
    //        request.source(stringMapping);
    //        bulkRequest.add(request);
    //
    //        try {
    //            BulkResponse indexResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    //        }
    //        catch(IOException ex)
    //        {
    //            ex.printStackTrace();
    //            System.exit(1);
    //        }
    //        }

    @Override
    protected long getSizeInBytes(String requestEntry) {
        return requestEntry.length();
    }
}
