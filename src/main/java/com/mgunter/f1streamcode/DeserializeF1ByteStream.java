package com.mgunter.f1streamcode;

import io.ppatierno.formula1.Driver;
import io.ppatierno.formula1.DriverDeserializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

public class DeserializeF1ByteStream {

  private SourceFunction<byte[]> source;
  private SinkFunction<String> sink;
  private static final Logger LOG = LoggerFactory.getLogger(DeserializeF1ByteStream.class);
  private static final ObjectMapper OM = new ObjectMapper();
  private static final DriverDeserializer driverDeserializer = new DriverDeserializer();

    private static String checkpointDir = "file:///tmp/flink/checkpoints";
    private static int windowSizeMins = 10;
    private static String driverName = "Antonio Giovannazi";
    private static int checkpointTimeout = 150000;
    private static boolean consoleOutput = true;
    private static int consoleOutputThreshold = 5;
    private static int kafkaPort = 31234; // very few "generated Ids" get to 10 clicks in 10 minutes,
    // so lowering the default threshold here to create less confusion.
    private static boolean startFromLatest = false;

  public DeserializeF1ByteStream(SourceFunction<byte[]> source, SinkFunction<String> sink) {
    this.source = source;
    this.sink = sink;
  }

  public void execute() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<byte[]> sensorStream = env.addSource(source);
    sensorStream
        .map(
            data -> {
              try {
                return driverDeserializer.deserialize("f1-telemetry-drivers", data);
              } catch (Exception e) {
                LOG.info("exception reading data: " + data);
                return null;
              }
            })
        .filter(Objects::nonNull)
        .keyBy(value -> value.getParticipantData().getRaceNumber())
        .map(
            new MapFunction<Driver, String>() {
              @Override
              public String map(Driver value) {
                return "  driverstatus:"
                    + value.getLapData().getDriverStatus()
                    + " car-position:"
                    + value.getLapData().getCarPosition()
                    + " curr-lap:"
                    + value.getLapData().getCurrentLapNum()
                    + " speed:"
                    + value.getCarTelemetryData().getSpeed()
                    + " fuel remaining in laps:"
                    + value.getLapData().getCurrentLapNum()
                    + value.getCarStatusData().getFuelRemainingLaps()
                    + " fuel in tank:"
                    + value.getCarTelemetryData().getSpeed()
                    + value.getCarStatusData().getFuelInTank()
                    + " driver:"
                    + value.getParticipantData().getDriverId(); // + value.toString();
              }
            })
        .addSink(sink);

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty("bootstrap.servers", "kafka-service:31234");

    // Create Kafka Consumer
    System.out.println("instantiating Kafka Consumer");
    // ArrayList<RaceInfo> raceInfoArray = new ArrayList<RaceInfo>();

    FlinkKafkaConsumer<byte[]> kafkaConsumer =
        new FlinkKafkaConsumer<>(
            "f1-telemetry-drivers",
            new AbstractDeserializationSchema<byte[]>() {
              @Override
              public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
              }
            },
            kafkaProperties);

    kafkaConsumer.setStartFromEarliest();
    DeserializeF1ByteStream job =
        new DeserializeF1ByteStream(kafkaConsumer, new PrintSinkFunction<String>());
    job.execute();
  }

    private void setJobParameters(String propertiesFile) throws IOException {
        //Load parameters from the specified src/main/resources/{}.properties file
        ParameterTool parameter =
                ParameterTool.fromPropertiesFile(getFileFromResourceAsStream(propertiesFile));
        // initialize defaults from poperties file
        consoleOutput = parameter.getBoolean("ConsoleOutput");
        consoleOutputThreshold = parameter.getInt("ConsoleOutputThreshold");
        windowSizeMins = parameter.getInt("WindowSizeMins");
        kafkaPort = parameter.getInt("KafkaPort");
        startFromLatest = parameter.getBoolean("startFromLatest");
        driverName = parameter.get("DriverName");
        checkpointDir = parameter.get("CheckpointDir");
        checkpointTimeout = parameter.getInt("CheckpointTimeout");

        LOG.info(
                "StreamingJob configured with parameter file: \n"
                        + propertiesFile
                        + " \n"
                        + parameter.getConfiguration().toString());
    }

    private InputStream getFileFromResourceAsStream(String fileName) {
        URL url = com.google.common.io.Resources.getResource(fileName);
        try {
            return com.google.common.io.Resources.newInputStreamSupplier(url).getInput();
        } catch (IOException ex) {
            LOG.error(ex.toString());
            throw new RuntimeException(ex);
        }
    }
}
