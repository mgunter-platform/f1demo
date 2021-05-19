package com.acmecorp;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class ParallelCollectionSource<String> extends RichParallelSourceFunction<String>
    implements ResultTypeQueryable {

  private List<Tuple2<String, Long>> input;
  private List<Tuple2<String, Long>> inputOfSubtask;

  public ParallelCollectionSource(List<Tuple2<String, Long>> input) {
    this.input = input;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

    inputOfSubtask = new ArrayList<>();

    for (int i = 0; i < input.size(); i++) {
      if (i % numberOfParallelSubtasks == indexOfThisSubtask) {
        inputOfSubtask.add(input.get(i));
      }
    }
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    for (Tuple2<String, Long> clickStreamListWithTimestamp : inputOfSubtask) {
      ctx.collectWithTimestamp(
          (String) clickStreamListWithTimestamp.f0, clickStreamListWithTimestamp.f1);
    }
  }

  @Override
  public void cancel() {
    // ignore cancel, finite anyway
  }

  @Override
  public TypeInformation getProducedType() {
    TypeInformation<List<Tuple2<String, Long>>> info =
        new TypeHint<List<Tuple2<String, Long>>>() {}.getTypeInfo();
    return info;
  }
}
