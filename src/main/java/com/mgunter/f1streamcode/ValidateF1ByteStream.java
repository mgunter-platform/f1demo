package com.mgunter.f1streamcode;

import io.ppatierno.formula1.DriverDeserializer;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ValidateF1ByteStream {

    private SourceFunction<byte[]> source;
    private SinkFunction<String> sink;
    private static final Logger LOG = LoggerFactory.getLogger(ValidateF1ByteStream.class);
    private static final ObjectMapper OM = new ObjectMapper();
    private static final DriverDeserializer driverDeserializer = new DriverDeserializer();


    public  ValidateF1ByteStream(
            SourceFunction<byte[]> source, SinkFunction<String> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<byte[]> sensorStream = env.addSource(source);
       /* DataStream<List<Integer>> integerListsStream =
                env.addSource(source)
                        .returns(TypeInformation.of(new TypeHint<byte[]>() {}))
                        .assignTimestampsAndWatermarks(
                                new BoundedOutOfOrdernessWatermarkAssigner<>(Time.of(100, TimeUnit.MILLISECONDS)));

        */
        sensorStream
                /*.map(data -> {
                    try {
                        return Arrays.toString(data);
                    } catch (Exception e) {
                        LOG.info("exception reading data: " + data);
                        return null;
                    }
                })*/
                .writeAsText("bytes.txt", FileSystem.WriteMode.OVERWRITE);
                //.addSink(sink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "kafka-service:31234");

        // Create Kafka Consumer
        System.out.println("instantiating Kafka Consumer");
        //ArrayList<RaceInfo> raceInfoArray = new ArrayList<RaceInfo>();

        FlinkKafkaConsumer<byte[]> kafkaConsumer =
                new FlinkKafkaConsumer<>("f1-telemetry-drivers", new AbstractDeserializationSchema<byte[]>() {
                    @Override
                    public byte[] deserialize(byte[] bytes) throws IOException {
                        return bytes;
                    }
                },  kafkaProperties);

        kafkaConsumer.setStartFromEarliest();
        ValidateF1ByteStream job = new ValidateF1ByteStream(kafkaConsumer, new PrintSinkFunction<String>());
        job.execute();
    }
}
