package com.acmecorp.processing.functions.test;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import com.acmecorp.processing.functions.FirstFieldOutputMapper;;

/**
 * Unit test for FirstFieldOutputMapper class.
 */
public class FirstFieldOutputMapperTest{
    /**
     * Test01: Tuple2<String, true> should give back the String value  
     */
    @Test
    public void testFirstFieldOutputMapperWithStringTrue() throws Exception{
    	FirstFieldOutputMapper<String> firstFieldOutputMapper = new FirstFieldOutputMapper<String>();
        String userId = "123456";
        assertEquals(userId, firstFieldOutputMapper.map(new Tuple2<>(userId,true)));
    }
    /**
     * Test01: Tuple2<String, false> should give back the String value  
     */
    @Test
    public void testFirstFieldOutputMapperWithStringFalse() throws Exception{
    	FirstFieldOutputMapper<String> firstFieldOutputMapper = new FirstFieldOutputMapper<String>();
        String userId = "123456";
        assertEquals(userId, firstFieldOutputMapper.map(new Tuple2<>(userId,true)));
    }
    /**
     * Test03: Tuple2<Integer, true> should give back the Integer value 
     */
    @Test
    public void testFirstFieldOutputMapperWithIntegerTrue() throws Exception{
    	FirstFieldOutputMapper<Integer> firstFieldOutputMapper = new FirstFieldOutputMapper<Integer>();
        Integer userId = 123456;
        assertEquals(userId, firstFieldOutputMapper.map(new Tuple2<>(userId,true)));
    }
    /**
     * Test04: Tuple2<Integer, false> should give back the Integer value 
     */
    @Test
    public void testFirstFieldOutputMapperWithIntegerFalse() throws Exception{
    	FirstFieldOutputMapper<Integer> firstFieldOutputMapper = new FirstFieldOutputMapper<Integer>();
        Integer userId = 123456;
        assertEquals(userId, firstFieldOutputMapper.map(new Tuple2<>(userId,false)));
    }
}
