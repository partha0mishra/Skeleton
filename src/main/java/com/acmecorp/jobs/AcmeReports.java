package com.acmecorp.jobs;
/**
 * Java implementation for the real-time processing of user data for Acme corporation.
 * The task of the exercise is to analyze the web server log of AcmeCorp, and answer the below questions
 * 
 * 1. How many actions a user is performing in a window of 10 mins. Update this information every 1 min.
 * 2. Output a directory with user IDs who performed 10 or more events in past 10 mins
 * 3. Output a directory with user IDs who performed less than 10 events in past 10 mins
 * 
 * Parameters:
 *   -all path-to-directory-for-unfiltered-records-of-users-and-events [e.g. hdfs://localhost:9000/acme/activities/report/all]
 *   -more path-to-directory-for-users-with-10-or-more-events [e.g. hdfs://localhost:9000/acme/activities/report/more]
 *   -less path-to-directory-for-users-with-less-events [e.g. hdfs://localhost:9000/acme/activities/report/less]
 *   -checkpoint path-to-directory-to-save-state [e.g. hdfs://localhost:9000/acme/activities/checkpoint]
 *   
 * One way to start this job can be:
 * flink run -c com.acmecorp.jobs.AcmeReports target/exercise-skeleton-1.0-SNAPSHOT.jar 
 * --all hdfs://localhost:9000/acme/activities/report/all 
 * --more hdfs://localhost:9000/acme/activities/report/more 
 * --less hdfs://localhost:9000/acme/activities/report/less 
 * --checkpoint hdfs://localhost:9000/acme/activities/checkpoint
 */

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

import com.acmecorp.processing.functions.FilterFalse;
import com.acmecorp.processing.functions.FilterTrue;
import com.acmecorp.processing.functions.FirstFieldOutputMapper;
import com.acmecorp.processing.functions.Splitter;
import com.acmecorp.processing.functions.TenOrLessFlatMapper;
import com.acmecorp.provided.ClickEventGenerator;


/**
 * @author partha_m
 *
 */
public class AcmeReports {
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		
		// Directory to show all user activities
		final String allDir = pt.getRequired("all");
		
		// Two directories to keep the user Ids with less than 10 events in a window and the one with others
		final String moreDir = pt.getRequired("more");
		final String lessDir = pt.getRequired("less");
		
		// Checkpoint directory
		final String checkpointDir = pt.getRequired("checkpoint");
		
		// Computation is preferred to be on Event Time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// Enabling checkpointing
		env.setStateBackend(new FsStateBackend(checkpointDir));
		env.enableCheckpointing(1000);
		
		// Restart strategy, to try 60 times after a 10 second time-out
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
		
		// Getting Data Stream from the generator class
		DataStream<String> input = env.addSource(new ClickEventGenerator(pt));
		// Split the json record, take user id, keyBy user id, sliding window of 10 mins with 1 min slide
		// Allowing 1 min late arrival
		// reduce(_+_)
		DataStream<Tuple2<String,Integer>> tenMinUserAction = input
				.flatMap(new Splitter())
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(1)))
				.allowedLateness(Time.minutes(1))
				//.sum(1) commenting. opting for a more flexible reduce function
				.reduce(new ReduceFunction<Tuple2<String,Integer>>(){
					private static final long serialVersionUID = 1L;
					public Tuple2<String,Integer> reduce(Tuple2<String,Integer> val1, Tuple2<String,Integer> val2){
						return new Tuple2<>(val1.f0,val1.f1+val2.f1);
					}
				});

		// Emit <user-id, false> if the number of events is less than 10. Emit <user-id, true> otherwise
		DataStream<Tuple2<String, Boolean>> tenOrLessUserActions=tenMinUserAction.flatMap(new TenOrLessFlatMapper<String>());
		
		// Take the user-ids with 'true' flag
		DataStream<String> moreActions = tenOrLessUserActions
				.filter(new FilterTrue<String>())
				.map(new FirstFieldOutputMapper<String,Boolean>());
		
		// Take the user-ids with 'false' flag
		DataStream<String> lessAction = tenOrLessUserActions
				.filter(new FilterFalse<String>())
				.map(new FirstFieldOutputMapper<String,Boolean>());
		//tenMinUserAction.print();

		// Rolling sink for ALL the events
		BucketingSink<Tuple2<String,Integer>> allSink = new BucketingSink<Tuple2<String,Integer>>(allDir)
				.setBucketer(new DateTimeBucketer<Tuple2<String,Integer>>("yyyy-MM-dd--HHmm"));

		// Rolling sink for events with 10 or more counts
		BucketingSink<String> moreSink = new BucketingSink<String>(moreDir)
				.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
		
		// Rolling sink for events with <10 counts
		BucketingSink<String> lessSink = new BucketingSink<String>(lessDir)
				.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
		
		// Attach Sinks to the DataStreams
		tenMinUserAction.addSink(allSink);
		moreActions.addSink(moreSink);
		lessAction.addSink(lessSink);
		
		// execute program
		env.execute("ACME Streaming Analytics");
	}
}
