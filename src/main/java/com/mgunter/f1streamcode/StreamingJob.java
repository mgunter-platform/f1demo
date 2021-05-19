package com.mgunter.f1streamcode;

import com.mgunter.provided.ClickEventGenerator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class StreamingJob {
  private SourceFunction<String> source;
  private SinkFunction<String> tenclicksink;
  private SinkFunction<String> nineclicksink;
  private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

  private static String checkpointDir = "file:///tmp/flink/checkpoints";
  private static int windowSizeMins = 10;
  private static int windowSlideSecs = 60;
  private static int checkpointTimeout = 150000;
  private static boolean consoleOutput = true;
  private static int consoleOutputCountThreshold = 5;
  private static int tenOrMoreThreshold = 6; // very few "generated Ids" get to 10 clicks in 10 minutes,
          // so lowering the default threshold here to create less confusion.
  private static int nineOrLessThreshold = 9;

  private static long skippedCount = 0;

  public StreamingJob(
      SourceFunction<String> source,
      SinkFunction<String> tenclicksink,
      SinkFunction<String> nineclicksink,
      String
          requestedConfigFile) { // to support multiple configurations profiles (e.g. Test,
                                 // Production, etc.)

    this.source = source;
    this.tenclicksink = tenclicksink;
    this.nineclicksink = nineclicksink;

    String propertiesFile = "StreamingJob.properties";
    if (requestedConfigFile != null) {
      propertiesFile = requestedConfigFile;
    }

    try {
      setJobParameters(propertiesFile);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      LOG.warn(
          " Configuration file not found at "
              + propertiesFile
              + " ,  Running with Default job config settings!");
    }
  }

  public void execute() throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(15000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
    env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.setStateBackend(new FsStateBackend(checkpointDir));

    env.getConfig().setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(Duration.ofMillis(100).toMillis());

    DataStream<String> input = env.addSource(source);

    DataStream<Tuple3<String, Integer, Integer>> output =
        input
            .map( new JSONStringMapper()) // map the JSON clickinfo string to a tuple3 <clickinfo,
                                        // accountId, counter=1>
            .returns( TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {}))
            .filter( new MainFilterFunction())
            .keyBy(1)
            .window( SlidingEventTimeWindows.of(
                     Time.minutes(windowSizeMins), Time.seconds(windowSlideSecs)))
            .sum(2);

    if (consoleOutput) {
      DataStreamSink consolePrint =
          output
              .filter(
                  new FilterFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, Integer, Integer> record)
                        throws Exception {
                      if (record.f2 < consoleOutputCountThreshold) {
                        return false;
                      }
                      return true;
                    }
                  })
              .addSink(new PrintSinkFunction<Tuple3<String, Integer, Integer>>())
              .name("Console output");
    }

    DataStreamSink tenOrMoreClicks =
        output
            .filter(
                new FilterFunction<Tuple3<String, Integer, Integer>>() {
                  @Override
                  public boolean filter(Tuple3<String, Integer, Integer> record) throws Exception {
                    if (record.f2 < tenOrMoreThreshold) {
                      return false;
                    }
                    return true;
                  }
                })
            .map(new TupleMapper())
            .addSink(tenclicksink)
            .name("TenClickSink");

    DataStreamSink nineOrLessClicks =
        output
            .filter(
                new FilterFunction<Tuple3<String, Integer, Integer>>() {
                  @Override
                  public boolean filter(Tuple3<String, Integer, Integer> record) throws Exception {
                    if (record.f2 >= nineOrLessThreshold) {
                      return false;
                    }
                    return true;
                  }
                })
            .map(
                new TupleMapper()) // map the tuple back into a single string of accountId for
                                   // writing to the file
            .addSink(nineclicksink)
            .name("NineClickSink");

    env.execute();
  }

  public static void main(String[] args) throws Exception {

    //setup directory location for the TenClick User ids
    String outputPath1 = "Tenclickpath/";
    final StreamingFileSink<String> tenclicksink =
        StreamingFileSink.forRowFormat(
                new Path(outputPath1), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(3))
                    .withMaxPartSize(1024 * 1024 * 1024)
                    .build())
            .withBucketAssigner(
                new BucketAssigner<String, String>() {
                  @Override
                  public String getBucketId(String s, Context context) {
                    return "Tenclickusers";
                  }

                  @Override
                  public SimpleVersionedSerializer<String> getSerializer() {
                    return SimpleVersionedStringSerializer.INSTANCE;
                  }
                })
            .build();

    //setup directory location for the Nine or Less-Click User ids
    String outputPath2 = "Nineclickpath/";
    final StreamingFileSink<String> nineclicksink =
        StreamingFileSink.forRowFormat(
                new Path(outputPath2), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(3))
                    .withMaxPartSize(1024 * 1024 * 1024)
                    .build())
            .withBucketAssigner(
                new BucketAssigner<String, String>() {
                  @Override
                  public String getBucketId(String s, Context context) {
                    return "Nineclickusers";
                  }

                  @Override
                  public SimpleVersionedSerializer<String> getSerializer() {
                    return SimpleVersionedStringSerializer.INSTANCE;
                  }
                })
            .build();

    // parameters required by ClickEventGenerator, but not used, so empty parameters passed
    StreamingJob job =
        new StreamingJob(
            (RichParallelSourceFunction<String>)
                new ClickEventGenerator(ParameterTool.fromArgs(args)),
            tenclicksink,
            nineclicksink,
            null);

    // execute program
    job.execute();
  }

  private void setJobParameters(String propertiesFile) throws IOException {
    //Load parameters from the specified src/main/resources/{}.properties file
    ParameterTool parameter =
        ParameterTool.fromPropertiesFile(getFileFromResourceAsStream(propertiesFile));
    // initialize defaults from poperties file
    consoleOutput = parameter.getBoolean("ConsoleOutput");
    consoleOutputCountThreshold = parameter.getInt("ConsoleOutputCountThreshold");
    windowSizeMins = parameter.getInt("WindowSizeMins");
    windowSlideSecs = parameter.getInt("WindowSlideSecs");
    tenOrMoreThreshold = parameter.getInt("TenOrMoreThreshold");
    nineOrLessThreshold = parameter.getInt("NineOrLessThreshold");
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

  private static class MainFilterFunction
      implements FilterFunction<Tuple3<String, Integer, Integer>> {
    @Override
    public boolean filter(Tuple3<String, Integer, Integer> record) throws Exception {
      if (record.f1 != -1 && record.f1 != null) {
        return true; // avoiding noise, we only care about events of Auth'd Users
      }

      if (skippedCount++ == 500000) {
        LOG.info("filtered-out another 500,000 like this: " + record.f0);
        skippedCount = 0;
      }

      return false;
    }
  }

  private class JSONStringMapper implements MapFunction<String, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> map(String s) throws Exception {
      ObjectMapper objectMapper = new ObjectMapper();

      JsonNode jsonNode = null;
      Tuple3 tuple = null;
      try {
        jsonNode = objectMapper.readTree(s);
        if (jsonNode.get("user.accountId").canConvertToInt() == true) {
          tuple =
              new Tuple3<String, Integer, Integer>(
                  objectMapper.writeValueAsString(jsonNode),
                  jsonNode.get("user.accountId").asInt(),
                  1);
        }
      } catch (Exception e) {
        LOG.error("Error converting from String to Json" +e.toString());
        e.printStackTrace();
      }
      return tuple;
    }
  }

  private class TupleMapper implements MapFunction<Tuple3<String, Integer, Integer>, String> {

    @Override
    public String map(Tuple3<String, Integer, Integer> tuple) throws Exception {

      return tuple.f1.toString();
    }
  }
}
