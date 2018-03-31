package com.acmecorp;

import com.acmecorp.BatchJob.Splitter;
import com.acmecorp.provided.ClickEventGenerator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class StreamingJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);

		DataStream<String> input = env.addSource(new ClickEventGenerator(pt));
		DataStream<Tuple2<String,Integer>> second = input
				.flatMap(new Splitter())
				.keyBy(0)
				.window(SlidingProcessingTimeWindows.of(Time.minutes(10),Time.minutes(1)))
				.sum(1);	

		second.print();
		
		//BucketingSink<String> sink = new BucketingSink<String>("D:\\tmp\\sinks\\01");
				
		// execute program
		env.execute("Streaming Analytics");
	}
	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>>{
		final String USER_ACCOUNTID = "user.accountId";
		final String USER_IP = "user.ip";
		final String PROPERTY_DELIMITER = ",";
		final String FIELD_DELIMITER = ":";
		final String INVALID_USER_ACCOUNT_ID = "-1";
		final String DOUBLE_QUOTE = "\"";
		final String NOTHING = "";
		final String OPENING_BRACES = "{";
		final String CLOSING_BRACES = "}";
		
		@Override
		public void flatMap(String line, Collector<Tuple2<String,Integer>> out) {
			String userIp = null;
			String userAccountId = null;
			for (String token: line
					.replace(OPENING_BRACES,NOTHING)
					.replace(CLOSING_BRACES,NOTHING)
					.split(PROPERTY_DELIMITER)) {

				String [] pv = token.split(FIELD_DELIMITER);
				String property = pv[0].replaceAll(DOUBLE_QUOTE, NOTHING);
				if (property.equalsIgnoreCase(USER_IP)){
					userIp = pv[1].replaceAll(DOUBLE_QUOTE, NOTHING);
				}else if (property.equalsIgnoreCase(USER_ACCOUNTID)) {
					userAccountId = pv[1].replaceAll(DOUBLE_QUOTE, NOTHING);
				}else {
					break;
				}
			}
			if(userAccountId.trim().equalsIgnoreCase(INVALID_USER_ACCOUNT_ID)) {
				out.collect(new Tuple2<String, Integer>(userIp,1));
			}else {
				out.collect(new Tuple2<String, Integer>(userAccountId,1));
			}
		}
	}
}
