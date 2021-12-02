package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class AmazonOpenSearchSinkTest {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        consumerConfig.put(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");

        DataStream<String> stream =
                env.addSource(
                        new FlinkKinesisConsumer<>(
                                "input-stream", new SimpleStringSchema(), consumerConfig));


        ElementConverter<String, String> converter = ((element, context) -> element);

        stream.sinkTo(new AmazonOpenSearchSink<String>("my-index",
                "search-my-os-test-g6qoldzgwlhs4jjc33i63paffi.us-east-1.es.amazonaws.com",
                443, "https"));

        env.execute("opensearch sink test");
    }
}
