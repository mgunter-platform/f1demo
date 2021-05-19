package com.acmecorp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mgunter.f1streamcode.StreamingJob;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThat;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

//Unit Test the StreamJob with a simplified Stream of data & window parameters
public class StreamingJobTest {
  private ObjectMapper mapper;
  private long eventTimeMillis;

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Before
  public void setup() throws Exception {
    this.mapper = new ObjectMapper();
  }

  //Test the stream with a few generated, timestamped clicks, which should result in the AcccountId captured in sink2(nine Or less category)
  @Test
  public void testStreamingJobNineOrLess() throws Exception {

    long ts = Math.round(System.currentTimeMillis() / 1000) * 1000 - 1000;
    List<Tuple2<String, Long>> clicks = new ArrayList<Tuple2<String, Long>>();
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(88), ts += 100));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(88), ts += 100));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(88), ts += 100));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(88), ts));

    // Arrange
    StringCollectingSink sink1 = new StringCollectingSink();
    StringCollectingSink sink2 = new StringCollectingSink();

    // for each test, the sink result must be reset
    sink1.result.clear();
    sink2.result.clear();
    ParallelSourceFunction<String> source = new ParallelCollectionSource(clicks);
    StreamingJob job = new StreamingJob(source, sink1, sink2, "TestStreamingJob.properties");

    // Act
    job.execute();

    // Assert
    assertThat(sink2.result).containsExactlyInAnyOrder("88");
  }

  //Test the stream with a few generated, timestamped clicks, which should result in the AcccountId captured in sink1(ten or more click category)
  @Test
  public void testStreamingJobTenOrMore() throws Exception {

    long ts = Math.round(System.currentTimeMillis() / 1000) * 1000 - 1000;
    List<Tuple2<String, Long>> clicks = new ArrayList<Tuple2<String, Long>>();

    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));

    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts += 10));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(99), ts));

    // Arrange
    StringCollectingSink sink1 = new StringCollectingSink();
    StringCollectingSink sink2 = new StringCollectingSink();

    // for each test, the sink result must be reset
    sink1.result.clear();
    sink2.result.clear();

    ParallelSourceFunction<String> source = new ParallelCollectionSource(clicks);
    StreamingJob job = new StreamingJob(source, sink1, sink2, "TestStreamingJob.properties");

    // Act
    job.execute();

    // Assert
    assertThat(sink1.result).containsExactlyInAnyOrder("99");
    assertThat(sink2.result).containsExactlyInAnyOrder("99");
  }

  //Test the stream with a few generated, timestamped clicks, which should span two windows and
  // result in the AcccountId captured in sink1(ten or more click category) AND insink2(nine Or less category)
  @Test
  public void testStreamingJobTenSpanningWindows() throws Exception {

    long ts = Math.round(System.currentTimeMillis() / 1000) * 1000 - 50000;
    List<Tuple2<String, Long>> clicks = new ArrayList<Tuple2<String, Long>>();
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 0));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));

    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));

    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 7500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 500));

    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts += 500));
    clicks.add(new Tuple2<String, Long>(generateOneClickForUser(77), ts));

    // Arrange
    StringCollectingSink sink1 = new StringCollectingSink();
    StringCollectingSink sink2 = new StringCollectingSink();

    // for each test, the sink result must be reset
    sink1.result.clear();
    sink2.result.clear();

    ParallelSourceFunction<String> source = new ParallelCollectionSource(clicks);
    StreamingJob job = new StreamingJob(source, sink1, sink2,"TestStreamingJob.properties");

    // Act
    job.execute();

    // Assert

    assertThat(sink1.result).containsExactlyInAnyOrder("77");

    assertThat(sink2.result).containsExactlyInAnyOrder("77");

  }

  public String generateOneClickForUser(int user) throws JsonProcessingException {
    ObjectNode node = mapper.createObjectNode();

    long ts = System.currentTimeMillis();

    node.put("user.ip", "192.168.1.1");
    node.put("user.accountId", user);
    node.put("page", "someurl");
    node.put("host.timestamp", ts );
    node.put("host", "host");
    node.put("host.sequence", "sequence");
    String jsonString = mapper.writeValueAsString(node);

    return jsonString;
  }
}
