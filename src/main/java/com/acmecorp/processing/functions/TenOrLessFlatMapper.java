/**
 * 
 */
package com.acmecorp.processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author partha_m
 *
 */
public class TenOrLessFlatMapper implements FlatMapFunction<Tuple2<String,Integer>,Tuple2<String, Boolean>> {
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object, org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap(Tuple2<String,Integer> input, Collector<Tuple2<String, Boolean>> out) throws Exception {
		String outVal = input.f0;
		// for debug
		//outVal = outVal+"_"+input.f1;
		
		// Send False for count less than 10 and True otherwise. Use boolean field for filter to create two different streams 
		if(input.f1 < 10) {
			out.collect(new Tuple2<>(outVal,false));
		}else {
			out.collect(new Tuple2<>(outVal,true));
		}
	}

}
