package com.acmecorp;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import com.acmecorp.provided.ClickEventGenerator;
/**
 * Java implementation for the real-time processing of user data for Acme corporation.
 * The task of the exercise is to analyze the web server log of AcmeCorp, and answer the below questions
 * 
 * 1. How many actions a user is performing in a window of 10 mins. Update this information every 1 min.
 * 2. Output a directory with user IDs who performed 10 or more events in past 10 mins
 * 3. Output a directory with user IDs who performed less than 10 events in past 10 mins
 * 
 * Parameters:
 *   -more path-to-directory-for-users-with-10-or-more-events
 *   -less path-to-directory-for-users-with-less-events
 *   -checkpoint path-to-directory-to-save-state
 */

public class AcmeReports {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		final String more_dir = pt.getRequired("more");
		final String less_dir = pt.getRequired("less");
		final String checkpoint_dir = pt.getRequired("checkpoint");
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		env.setStateBackend(new FsStateBackend(checkpoint_dir));
		env.enableCheckpointing(1000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
		
		DataStream<String> input = env.addSource(new ClickEventGenerator(pt));
		DataStream<Tuple2<String,Integer>> tenMinUserAction = input
				.flatMap(new Splitter())
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(1)))
				.reduce(new ReduceFunction<Tuple2<String,Integer>>(){
					private static final long serialVersionUID = 1L;
					public Tuple2<String,Integer> reduce(Tuple2<String,Integer> val1, Tuple2<String,Integer> val2){
						return new Tuple2<>(val1.f0,val1.f1+val2.f1);
					}
				});

		DataStream<Tuple2<String, Boolean>> tenOrLessUserActions=tenMinUserAction.flatMap(new TenOrLessFlatMapper());
		DataStream<String> moreActions = tenOrLessUserActions
				.filter(new FilterTrue())
				.map(new FirstFieldOutputMapper());
		DataStream<String> lessAction = tenOrLessUserActions
				.filter(new FilterFalse())
				.map(new FirstFieldOutputMapper());
		tenMinUserAction.print();
/*
		BucketingSink<Tuple2<String,Integer>> sink = new BucketingSink<Tuple2<String,Integer>>("/acme/tmp00")
				//.setWriter(new SequenceFileWriter<Text,IntWritable>())
				.setBucketer(new DateTimeBucketer<Tuple2<String,Integer>>("yyyy-MM-dd--HHmm"));
		
		tenMinUserAction.addSink(sink);
*//*
		BucketingSink<String> moreSink = new BucketingSink<String>(more_dir)
				.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
		
		BucketingSink<String> lessSink = new BucketingSink<String>(less_dir)
				.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
		
		moreActions.addSink(moreSink);
		lessAction.addSink(lessSink);
		*/
		// execute program
		env.execute("ACME Streaming Analytics");
	}
}
