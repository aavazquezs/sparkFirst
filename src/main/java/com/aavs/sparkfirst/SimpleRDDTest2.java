package com.aavs.sparkfirst;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author angel
 */
public class SimpleRDDTest2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator accum = sc.sc().longAccumulator();
        sc.parallelize(Arrays.asList(1,2,3,4)).foreach(x->accum.add(x));
        System.out.println("Accumulate value: "+accum.value());
    }
}
