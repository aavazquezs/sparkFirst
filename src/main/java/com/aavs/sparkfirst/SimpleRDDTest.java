package com.aavs.sparkfirst;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author angel
 */
public class SimpleRDDTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        Integer count = distData.reduce((a,b)->a+b);
        System.out.println("Suma de los numeros: "+count);
    }
}
