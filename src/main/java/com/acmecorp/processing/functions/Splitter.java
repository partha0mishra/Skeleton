package com.acmecorp.processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author partha_m
 *
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

	/**
	 * The structure of record to be split is below
	 * 
	 * {
		"user.ip":"77.158.183.15",
		"user.accountId":-1,
		"page":"/blog/57949",
		"host.timestamp":1508507183439,
		"host":"lb-brl-8-3.globalcorp.com",
		"host.sequence":20682
		}
	 */
	private static final long serialVersionUID = 1L;
	final String USER_ACCOUNTID = "user.accountId";
	final String USER_IP = "user.ip";
	final String PROPERTY_DELIMITER = ",";
	final String FIELD_DELIMITER = ":";
	final String INVALID_USER_ACCOUNT_ID = "-1";
	final String DOUBLE_QUOTE = "\"";
	final String NOTHING = "";
	final String OPENING_BRACES = "{";
	final String CLOSING_BRACES = "}";

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object, org.apache.flink.util.Collector)
	 */
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
			// if account ID = -1, let's use userIp instead
			//out.collect(new Tuple2<String, Integer>(userIp,1));
		}else {
			out.collect(new Tuple2<String, Integer>(userAccountId.trim(),1));
		}
	}

}
