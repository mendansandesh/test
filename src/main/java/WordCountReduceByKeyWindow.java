package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WordCountReduceByKeyWindow {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCountReduceByKeyWindow");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK());
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairDStream<String, Integer> count = pairs.reduceByKeyAndWindow((v1, v2) -> (v1 + v2),Durations.seconds(30), Durations.seconds(10));

        count.print();
        ssc.start();
        ssc.awaitTermination();
    }

}
