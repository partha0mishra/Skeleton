package com.acmecorp.processing.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FirstFieldOutputMapper<T> implements MapFunction<Tuple2<T,Boolean>,T> {
	private static final long serialVersionUID = 1L;

	@Override
	public T map(Tuple2<T, Boolean> input) throws Exception {
		// Returns the first field as output
		return input.f0;
	}
}
