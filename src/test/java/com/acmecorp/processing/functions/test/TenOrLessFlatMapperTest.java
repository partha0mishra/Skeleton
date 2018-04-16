package com.acmecorp.processing.functions.test;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import com.acmecorp.processing.functions.TenOrLessFlatMapper;

/**
 * Unit test for TenOrLessFlatMapper class.
 */
public class TenOrLessFlatMapperTest{
    /**
     * Test01: ("12345",1) gives ("12345",false)  
     */
	@SuppressWarnings("unchecked")
	@Test
	public void testTenOrLessMapperVal1False() throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);
	    String userId = "12345";
	    Tuple2<String,Integer> input = new Tuple2<>(userId,1);
	    Tuple2<String,Boolean> result=new Tuple2<>(userId,false);
	    // values are collected in a static variable
	    CollectSink.values.clear();
	
	    // create a stream of custom elements and apply transformations
	    env.fromElements(input)
	            .flatMap(new TenOrLessFlatMapper<String>())
	            .addSink(new CollectSink());
	
	    // execute
	    env.execute();
	    

	    // Should get a blank []
        assertEquals(Lists.newArrayList(result), CollectSink.values);
	}
	/**
     * Test02: ("12345",9) gives ("12345",false)  
     */
	@SuppressWarnings("unchecked")
	@Test
	public void testTenOrLessMapperVal9False() throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);
	    String userId = "12345";
	    Tuple2<String,Integer> input = new Tuple2<>(userId,9);
	    Tuple2<String,Boolean> result=new Tuple2<>(userId,false);
	    // values are collected in a static variable
	    CollectSink.values.clear();
	
	    // create a stream of custom elements and apply transformations
	    env.fromElements(input)
	            .flatMap(new TenOrLessFlatMapper<String>())
	            .addSink(new CollectSink());
	
	    // execute
	    env.execute();
	    

	    // Should get a blank []
        assertEquals(Lists.newArrayList(result), CollectSink.values);
	}
	/**
     * Test03: ("12345",10) gives ("12345",true)  
     */
	@SuppressWarnings("unchecked")
	@Test
	public void testTenOrLessMapperVal10True() throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);
	    String userId = "12345";
	    Tuple2<String,Integer> input = new Tuple2<>(userId,10);
	    Tuple2<String,Boolean> result=new Tuple2<>(userId,true);
	    // values are collected in a static variable
	    CollectSink.values.clear();
	
	    // create a stream of custom elements and apply transformations
	    env.fromElements(input)
	            .flatMap(new TenOrLessFlatMapper<String>())
	            .addSink(new CollectSink());
	
	    // execute
	    env.execute();
	    

	    // Should get a blank []
        assertEquals(Lists.newArrayList(result), CollectSink.values);
	}
	/**
     * Test04: ("12345",11) gives ("12345",true)  
     */
	@SuppressWarnings("unchecked")
	@Test
	public void testTenOrLessMapperVal11True() throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);
	    String userId = "12345";
	    Tuple2<String,Integer> input = new Tuple2<>(userId,11);
	    Tuple2<String,Boolean> result=new Tuple2<>(userId,true);
	    // values are collected in a static variable
	    CollectSink.values.clear();
	
	    // create a stream of custom elements and apply transformations
	    env.fromElements(input)
	            .flatMap(new TenOrLessFlatMapper<String>())
	            .addSink(new CollectSink());
	
	    // execute
	    env.execute();
	    

	    // Should get a blank []
        assertEquals(Lists.newArrayList(result), CollectSink.values);
	}
	
	/**
	 * @author partha_m
	 *
	 */
	private static class CollectSink implements SinkFunction<Tuple2<String, Boolean>> {
		private static final long serialVersionUID = 1L;
		public static final List<Tuple2<String, Boolean>> values = new ArrayList<>();

		@Override
		public void invoke(Tuple2<String, Boolean> arg0) throws Exception {
			values.add(arg0);
		}
    }
}
