package com.acmecorp.processing.functions;

/**
 * Filter function that takes a Tuple2<T,Boolean> and if the Boolean flag is true, returns true.
 */
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author partha_m
 *
 * @param <T> Type of the data element
 */
public class FilterTrue<T> implements FilterFunction<Tuple2<T, Boolean>> {
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.FilterFunction#filter(java.lang.Object)
	 */
	@Override
	public boolean filter(Tuple2<T, Boolean> arg0) throws Exception {
		return arg0.f1;
	}

}
