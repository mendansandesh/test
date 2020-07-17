package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.util.Arrays;
import java.util.regex.Pattern;


public class NetworkWordCount {
    public static void main(String[] args) throws Exception{

        SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER());
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        /*wordCounts.foreachRDD(x -> {
            try{
                FileWriter fw=new FileWriter("E:\\test\\testout.txt");
                fw.write(x.rdd().toString());
                fw.close();
            }catch(Exception e){System.out.println(e);}
            System.out.println("Success...");
        });
        */
        //wordCounts.saveAsHadoopFiles("E:\\test");
        ssc.start();
        ssc.awaitTermination();
    }
}
