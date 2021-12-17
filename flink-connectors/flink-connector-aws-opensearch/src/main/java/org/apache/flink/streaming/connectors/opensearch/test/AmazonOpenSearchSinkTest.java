package org.apache.flink.streaming.connectors.opensearch.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.opensearch.AmazonOpenSearchSink;

import java.util.Properties;

/** */
public class AmazonOpenSearchSinkTest {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(6000);
        env.setParallelism(6);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-2");
        consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        DataStream<String> stream =
                env.addSource(
                        new FlinkKinesisConsumer<>(
                                "input-stream", new SimpleStringSchema(), consumerConfig));

        stream.sinkTo(
                new AmazonOpenSearchSink<>(
                        "https://search-playground-2ftaid4l2gqnvk2pbirluxblkq.us-east-1.es.amazonaws.com",
                        "admin",
                        "HappyClip#1",
                        "my-kds-events"
                ));

        env.execute();
    }
}
