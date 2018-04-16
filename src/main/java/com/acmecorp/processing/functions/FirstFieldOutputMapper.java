package com.acmecorp.processing.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author partha_m
 *
 */
public class FirstFieldOutputMapper<T,U> implements MapFunction<Tuple2<T,U>,T> {
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	@Override
	public T map(Tuple2<T, U> input) throws Exception {
		// Returns the first field as output
		return input.f0;
	}
}
