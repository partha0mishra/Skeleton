package com.acmecorp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FirstFieldOutputMapper implements MapFunction<Tuple2<String,Boolean>,String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String map(Tuple2<String, Boolean> input) throws Exception {
		// TODO Auto-generated method stub
		return input.f0;
	}

}
