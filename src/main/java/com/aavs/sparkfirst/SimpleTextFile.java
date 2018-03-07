package com.aavs.sparkfirst;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Word counting example
 * @author angel
 */
public class SimpleTextFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> distFile = sc.textFile("/home/angel/Desktop/Décima.txt");
        JavaPairRDD<String, Integer> counts = distFile.flatMap(s->Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word->new Tuple2<>(word,1))
                .reduceByKey((a,b)->a+b)
                .sortByKey();
        counts.saveAsTextFile("/home/angel/Desktop/counting");
        counts.take(10).stream().forEach(t->{System.out.println(t._1+": "+t._2);});
    }
}
