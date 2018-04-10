package com.acmecorp;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterTrue implements FilterFunction<Tuple2<String, Boolean>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Tuple2<String, Boolean> arg0) throws Exception {
		return arg0.f1;
	}

}
