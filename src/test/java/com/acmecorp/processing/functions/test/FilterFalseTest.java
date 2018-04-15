package com.acmecorp.processing.functions.test;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import com.acmecorp.processing.functions.FilterFalse;

/**
 * Unit test for FilterFalse class.
 */
public class FilterFalseTest{
    /**
     * Test01: Tuple2<String, true> should give back 'false'  
     */
    @Test
    public void testFilterFalseWithTrue() throws Exception{
        FilterFalse filterTrue = new FilterFalse();
        String userId = "123456";
        boolean result = false;
        assertEquals(result, filterTrue.filter(new Tuple2<>(userId,true)));
    }
    /**
     * Test02: Tuple2<String, false> should give back 'true'  
     */
    @Test
    public void testFilterFalseWithFalse() throws Exception{
        FilterFalse filterTrue = new FilterFalse();
        String userId = "123456";
        boolean result = true;
        assertEquals(result, filterTrue.filter(new Tuple2<>(userId,false)));
    }
}
