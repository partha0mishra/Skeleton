package com.acmecorp.processing.functions.test;

import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import com.acmecorp.processing.functions.FilterTrue;

/**
 * Unit test for FilterTrue class.
 */
public class FilterTrueTest{
    /**
     * Test01: Tuple2<String, true> should give back 'true'  
     */
    @Test
    public void testFilterTrueWithTrue() throws Exception{
        FilterTrue<String> filterTrue = new FilterTrue<String>();
        String userId = "123456";
        boolean result = true;
        assertEquals(result, filterTrue.filter(new Tuple2<>(userId,true)));
    }
    /**
     * Test02: Tuple2<String, false> should give back 'false'  
     */
    @Test
    public void testFilterTrueWithFalse() throws Exception{
        FilterTrue<String> filterTrue = new FilterTrue<String>();
        String userId = "123456";
        boolean result = false;
        assertEquals(result, filterTrue.filter(new Tuple2<>(userId,false)));
    }
 }
