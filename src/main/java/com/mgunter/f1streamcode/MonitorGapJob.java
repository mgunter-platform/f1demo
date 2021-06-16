package com.mgunter.f1streamcode;

import io.ppatierno.formula1.Driver;
import io.ppatierno.formula1.DriverDeserializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class MonitorGapJob {

    private SourceFunction<byte[]> source;
    private SinkFunction<String> sink;
    private static final Logger LOG = LoggerFactory.getLogger(com.mgunter.f1streamcode.FindFollowerJob.class);
    private static final ObjectMapper OM = new ObjectMapper();
    private static final DriverDeserializer driverDeserializer = new DriverDeserializer();


    public MonitorGapJob(
            SourceFunction<byte[]> source, SinkFunction<String> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TypeInformation.of(String.class).createSerializer(env.getConfig());
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<byte[]> sensorStream = env.addSource(source);
        sensorStream
                .map(data -> {
                    try {
                        return   driverDeserializer.deserialize("f1-telemetry-drivers",data);
                    } catch (Exception e) {
                        LOG.info("exception reading data: " + data);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .keyBy(MonitorGapJob::KeyFunction)
                // the function that evaluates the state machine over the sequence of events
                .flatMap(new FindFollowerMapper())
                .flatMap(new GapAnalysisMapper())
                .map(
                        new MapFunction<Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>, String>() {
                            @Override
                            public String map(Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean> value) {

                                //return new Short(value.getParticipantData().getYourTelemetry());
                                //=IF(AND(ABS(AVERAGE(M2065:M2068))>10000,Q2068>11,ABS(U2068)<0.06,ABS(AVERAGE(AC2065:AC2068))>0.18),"RS","T")

                                return "behind : " + value.f3.getDriverId() + " by "+ value.f1.getMetersToLeader()+"m or by " + value.f1.getSecsToLeader() + "s\n" +
                                        "ahead of : " + value.f2.getDriverId() + " by "+ value.f1.getMetersFromFollower()+"m or by " + value.f1.getSecsFromFollower() + "s\n" +
                                        "    (Current driver status:" + value.f0.getLapData().getDriverStatus()+" car-position:"+value.f0.getLapData().getCarPosition()+" curr-lap:"+value.f0.getLapData().getCurrentLapNum()+" speed:"+value.f0.getCarTelemetryData().getSpeed()+" fuel remaining in laps:"+value.f0.getLapData().getCurrentLapNum()+ "\n"+
                                        " fuel in tank:"+value.f0.getCarTelemetryData().getSpeed()+value.f0.getCarStatusData().getFuelInTank()+" driver:"+ value.f0.getParticipantData().getDriverId() +" ) ";//+ value.toString();
                            }
                        }).addSink(sink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "kafka-service:31234");

        // Create Kafka Consumer
        System.out.println("instantiating Kafka Consumer");
        //ArrayList<RaceInfo> raceInfoArray = new f1d   ArrayList<RaceInfo>();

        FlinkKafkaConsumer<byte[]> kafkaConsumer =
                new FlinkKafkaConsumer<>("f1-telemetry-drivers", new AbstractDeserializationSchema<byte[]>() {
                    @Override
                    public byte[] deserialize(byte[] bytes) throws IOException {
                        return bytes;
                    }
                },  kafkaProperties);

        kafkaConsumer.setStartFromEarliest();
        MonitorGapJob job = new MonitorGapJob(kafkaConsumer, new PrintSinkFunction<String>());
        job.execute();
    }

    @SuppressWarnings("serial")
    static public class FindFollowerMapper extends RichFlatMapFunction<Driver, Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>> {

        /** The state for the current key. */
        private ValueState<com.mgunter.f1streamcode.RaceInfo> mgState;
        private ValueState<com.mgunter.f1streamcode.RaceInfo> followerState;
        private ValueState<com.mgunter.f1streamcode.RaceInfo> leaderState;
        private ValueState<Boolean> tracking;
        private static String driverId = "ANTONIO_GIOVINAZZI";

        @Override
        public void open(Configuration conf) {
            // get access to the state object
            mgState =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("mgstate", com.mgunter.f1streamcode.RaceInfo.class));
            followerState=
                    getRuntimeContext().getState(new ValueStateDescriptor<>("followerstate", com.mgunter.f1streamcode.RaceInfo.class));
            leaderState=
                    getRuntimeContext().getState(new ValueStateDescriptor<>("leaderstate", com.mgunter.f1streamcode.RaceInfo.class));
            tracking=
                    getRuntimeContext().getState(new ValueStateDescriptor<>("tracking", Boolean.class));
        }

        @Override
        public void flatMap(Driver evt, Collector<Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>> out) throws Exception {

            //Init persistent values first time thru
            if (mgState.value() == null) {
                com.mgunter.f1streamcode.RaceInfo tempRaceInfo = new com.mgunter.f1streamcode.RaceInfo("NULL");
                tempRaceInfo.setCarPosition(0);
                tempRaceInfo.setLapPosition((float) 0);
                mgState.update(tempRaceInfo);

            }
            if (followerState.value() == null) {
                followerState.update(new com.mgunter.f1streamcode.RaceInfo("NOFOLLOWER"));
            }
            if (leaderState.value() == null) {
                leaderState.update(new com.mgunter.f1streamcode.RaceInfo("NOLEADER"));
            }
            if (tracking.value() == null) {
                tracking.update(new Boolean(false));
            }


            //evt.getParticipantData().setName(this.tracking.value().toString()); //comment out after debugging!

            com.mgunter.f1streamcode.RaceInfo mgRaceInfo = mgState.value();
            // Everytime we see MATT_GUNTER we update state

            if (evt.getParticipantData().getDriverId().toString().equalsIgnoreCase(driverId) ) {
                mgRaceInfo = new com.mgunter.f1streamcode.RaceInfo(evt.getParticipantData().getDriverId().toString());
                mgRaceInfo.setDriverDetails(evt);
                mgRaceInfo.setLap( new Short(evt.getLapData().getCurrentLapNum() ).intValue() );
                mgRaceInfo.setLapPosition( evt.getLapData().getLapDistance() );
                mgRaceInfo.setCarPosition( new Short(evt.getLapData().getCarPosition() ).intValue() );
                mgRaceInfo.setSpeedInKph(evt.getCarTelemetryData().getSpeed());
                mgState.update(mgRaceInfo);

            }

            com.mgunter.f1streamcode.RaceInfo followerRaceInfo = followerState.value();
            if (followerRaceInfo.getCarPosition() <= mgRaceInfo.getCarPosition()) {
                followerRaceInfo.setDriverId("NOFOLLOWER");
            }
            // Everytime we see a New or Closer Follower we update state
            if ( !(evt.getParticipantData().getDriverId().toString().equalsIgnoreCase(driverId ))
                    && (followerRaceInfo.getDriverId() == "NOFOLLOWER") && mgRaceInfo.getCarPosition()!=0
                    && mgRaceInfo.getCarPosition()< (new Short(evt.getLapData().getCarPosition() ).intValue())) {
                followerRaceInfo = new com.mgunter.f1streamcode.RaceInfo(evt.getParticipantData().getDriverId().toString());
                followerRaceInfo.setDriverDetails(evt);
                followerRaceInfo.setLap( new Short(evt.getLapData().getCurrentLapNum() ).intValue() );
                followerRaceInfo.setLapPosition( evt.getLapData().getLapDistance() );
                followerRaceInfo.setCarPosition( new Short(evt.getLapData().getCarPosition() ).intValue() );
                followerRaceInfo.setSpeedInKph(evt.getCarTelemetryData().getSpeed());
                followerState.update(followerRaceInfo);
                //this.tracking.update(true);
            }
            if ( !(evt.getParticipantData().getDriverId().toString().equalsIgnoreCase(driverId ))
                    && evt.getLapData().getCarPosition() >0
                    && evt.getLapData().getCarPosition() > mgRaceInfo.getCarPosition()
                    && mgRaceInfo.getCarPosition()!=0
                    && (evt.getLapData().getCarPosition() <= followerRaceInfo.getCarPosition()))
            {
                followerRaceInfo = new com.mgunter.f1streamcode.RaceInfo(evt.getParticipantData().getDriverId().toString());
                followerRaceInfo.setDriverDetails(evt);
                followerRaceInfo.setLap( new Short(evt.getLapData().getCurrentLapNum() ).intValue() );
                followerRaceInfo.setLapPosition( evt.getLapData().getLapDistance() );
                followerRaceInfo.setCarPosition( new Short(evt.getLapData().getCarPosition() ).intValue() );
                followerRaceInfo.setSpeedInKph(evt.getCarTelemetryData().getSpeed());
                followerState.update(followerRaceInfo);
                this.tracking.update(new Boolean(true));


            }


            com.mgunter.f1streamcode.RaceInfo leaderRaceInfo = leaderState.value();
            if (leaderRaceInfo.getCarPosition() >= mgRaceInfo.getCarPosition()) {
                leaderRaceInfo.setDriverId("NOLEADER");
            }
            // Everytime we see a New or Closer Follower we update state
            if ( !(evt.getParticipantData().getDriverId().toString().equalsIgnoreCase(driverId ))
                    && mgRaceInfo.getCarPosition()!=0
                    && (leaderRaceInfo.getDriverId() == "NOLEADER")
                    && mgRaceInfo.getCarPosition()> (new Short(evt.getLapData().getCarPosition() ).intValue())) {
                leaderRaceInfo = new com.mgunter.f1streamcode.RaceInfo(evt.getParticipantData().getDriverId().toString());
                leaderRaceInfo.setDriverDetails(evt);
                leaderRaceInfo.setLap( new Short(evt.getLapData().getCurrentLapNum() ).intValue() );
                leaderRaceInfo.setLapPosition( evt.getLapData().getLapDistance() );
                leaderRaceInfo.setCarPosition( new Short(evt.getLapData().getCarPosition() ).intValue() );
                leaderRaceInfo.setSpeedInKph(evt.getCarTelemetryData().getSpeed());
                leaderState.update(leaderRaceInfo);
                //this.tracking.update(true);
            }
            if ( !(evt.getParticipantData().getDriverId().toString().equalsIgnoreCase(driverId ))
                    && mgRaceInfo.getCarPosition()!=0
                    && evt.getLapData().getCarPosition() >0
                    && evt.getLapData().getCarPosition() < mgRaceInfo.getCarPosition()
                    && (evt.getLapData().getCarPosition() >= leaderRaceInfo.getCarPosition()))
            {
                leaderRaceInfo = new com.mgunter.f1streamcode.RaceInfo(evt.getParticipantData().getDriverId().toString());
                leaderRaceInfo.setDriverDetails(evt);
                leaderRaceInfo.setLap( new Short(evt.getLapData().getCurrentLapNum() ).intValue() );
                leaderRaceInfo.setLapPosition( evt.getLapData().getLapDistance() );
                leaderRaceInfo.setCarPosition( new Short(evt.getLapData().getCarPosition() ).intValue() );
                leaderRaceInfo.setSpeedInKph(evt.getCarTelemetryData().getSpeed());
                leaderState.update(leaderRaceInfo);
                this.tracking.update(new Boolean(true));


            }

            if (( tracking.value().booleanValue()) && (mgRaceInfo.getDriverId().toString().equalsIgnoreCase(evt.getParticipantData().getDriverId().toString()) ||
                    followerRaceInfo.getDriverId().toString().equalsIgnoreCase(evt.getParticipantData().getDriverId().toString()) ||
                    leaderRaceInfo.getDriverId().toString().equalsIgnoreCase(evt.getParticipantData().getDriverId().toString()) )) {

                out.collect(new Tuple5<>(evt, mgRaceInfo,followerRaceInfo,leaderRaceInfo,tracking.value()));
            }





            // ask the state machine what state we should go to based on the given event

        }
    }
    static public class GapAnalysisMapper extends RichFlatMapFunction<Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>, Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>> {

        /** The state for the current key. */
        Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean> currentSet;
        private com.mgunter.f1streamcode.RaceInfo mgState;
        private com.mgunter.f1streamcode.RaceInfo followerState;
        private com.mgunter.f1streamcode.RaceInfo leaderState;


        @Override
        public void open(Configuration conf) {
            // get access to the state object

        }

        @Override
        public void flatMap(Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean> evt, Collector<Tuple5<Driver, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo, com.mgunter.f1streamcode.RaceInfo,Boolean>> out) throws Exception {
            //Init persistent values first time thru
            currentSet = evt;

            mgState = evt.f1;
            followerState =  evt.f2;
            leaderState = evt.f3;

            try {
                mgState.setMetersFromFollower(mgState.getLapPosition() - followerState.getLapPosition());
                mgState.setSecsFromFollower((float) (mgState.getMetersFromFollower()
                        / ( 0.278 * followerState.getSpeedInKph())));

                mgState.setMetersToLeader( leaderState.getLapPosition() - mgState.getLapPosition());
                mgState.setSecsToLeader((float) (mgState.getMetersToLeader()
                        / (0.278 * leaderState.getSpeedInKph())));
            } catch (Exception e) {
                e.printStackTrace();
            }

            currentSet.setField(mgState,1);
            out.collect(currentSet);

        }
    }
    private static int KeyFunction (Driver d) {
        return 0;
    }

}
