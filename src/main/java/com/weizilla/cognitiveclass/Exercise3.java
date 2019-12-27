package com.weizilla.cognitiveclass;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercise3 {
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = context.textFile(inputFile);
            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
            JavaPairRDD<String, Integer> wordWithOnes = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairRDD<String, Integer> wordWithCounts = wordWithOnes.reduceByKey((a, b) -> a + b);
            List<Tuple2<String, Integer>> collect = wordWithCounts.collect();
            collect.forEach(System.out::println);
        }
    }
}
