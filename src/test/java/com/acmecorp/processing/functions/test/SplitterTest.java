package com.acmecorp.processing.functions.test;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import com.acmecorp.processing.functions.Splitter;

/**
 * Unit test for Splitter class.
 */
public class SplitterTest{
    /**
     * Test01: user.accountId = -1 gives null  
     */
	@Test
	public void testSplitterUnauthorizedAccountId() throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);
	    String line = "{\"user.ip\":\"77.158.183.15\",\"user.accountId\":-1,\"page\":\"/blog/57949\",\"host.timestamp\":1508507183439,\"host\":\"lb-brl-8-3.globalcorp.com\",\"host.sequence\":20682}";
	    // values are collected in a static variable
	    CollectSink.values.clear();
	
	    // create a stream of custom elements and apply transformations
	    env.fromElements(line)
	            .flatMap(new Splitter())
	            .addSink(new CollectSink());
	
	    // execute
	    env.execute();

	    // Should get a blank []
        assertEquals(Lists.newArrayList(), CollectSink.values);
	}
	/**
     * Test02: user.accountId = 12345 gives (12345,1)
     */
	@SuppressWarnings("unchecked")
	@Test
    public void testSplitterAuthorizedAccountId() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String line = "{\"user.ip\":\"77.158.183.15\",\"user.accountId\":12345,\"page\":\"/blog/57949\",\"host.timestamp\":1508507183439,\"host\":\"lb-brl-8-3.globalcorp.com\",\"host.sequence\":20682}";
        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(line)
                .flatMap(new Splitter())
                .addSink(new CollectSink());

        // execute
        env.execute();

        Tuple2<String,Integer> result = new Tuple2<>("12345",1);
        //Should get an element with ("12345",1)
        assertEquals(Lists.newArrayList(result), CollectSink.values);
    }
	
	private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		public static final List<Tuple2<String, Integer>> values = new ArrayList<>();

		@Override
		public void invoke(Tuple2<String, Integer> arg0) throws Exception {
			values.add(arg0);
		}
    }
}
