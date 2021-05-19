package com.acmecorp;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringCollectingSink implements SinkFunction<String> {
  int counter = 0;
  public static final List<String> result = Collections.synchronizedList(new ArrayList<>());

  public void invoke(String value, Context context) throws Exception {

    result.add(value);
  }
}
