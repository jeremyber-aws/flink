package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;


import java.util.Properties;


public class AmazonOpenSearchSinkTest {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(new Configuration());

        env.enableCheckpointing(6000);
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while(true) {

                    // create X number of records per second
                    int x = 100;
                    for(int i = 0; i<x; i++)
                    {
                        sourceContext.collect(String.valueOf(i) + "jeremy");
                    }
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });


        stream.sinkTo(new AmazonOpenSearchSink<String>("my-index-123-jeremyabc",
                "search-unrestrictive-os-pgeiaotahqdflj3xxzvtx2lbju.us-east-1.es.amazonaws.com",
                443, "https"));

        env.execute();
    }
}

