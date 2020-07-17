package main.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;

public class Dummy implements java.io.Serializable {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("AppSec Analytics")
                .setMaster("local[*]");
                //.set("couchbase.ipPortList", args.get("couchbase.ipPortList"))
                //.set("couchbase.userId", args.get("couchbase.userId"))
                //.set("couchbase.password", args.get("couchbase.password"))
                //.set("kafka.bootstrap.servers", args.get("kafka.bootstrap.servers"))
                //.set("spark.streaming.kafka.consumer.cache.enabled", "false");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));

        

        /*stream.foreachRDD(x -> {
            //System.out.println("--- New RDD with " + x.partitions().size() + " partitions and " + x.count() + " records");
            x.foreach(record -> System.out.println("record toString()..." + record.toString()));
        });*/

        
        
        JavaPairDStream<String,String> a =  getUserEvents(ssc)
                                            .mapToPair(getUserEventsDummy());
        a.print();
        /*Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.11.96.84:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        Set<String> topics = Collections.singleton("associate-process-service-appln");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,kafkaParams));*/
        
        
        ssc.start();
        ssc.awaitTermination();
        /*Set<String> topics = Collections.singleton("associate-process-service-appln");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("auto.offset.reset", "latest");
        //JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);*/
    }

    private static PairFunction<Tuple2<String,String>,String,String> getUserEventsDummy() {
        return dummy -> {
            System.out.println("record2 toString()..." + dummy._1);
            return dummy;
        };
    }

    private static JavaPairDStream<String, String> getUserEvents(JavaStreamingContext ssc) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.11.96.84:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        kafkaParams.put("linger.ms",3000);
        kafkaParams.put("buffer.memory",67108864);
        kafkaParams.put("retries", 5);
        kafkaParams.put("retry.backoff.ms",1000);


        Set<String> topics = Collections.singleton("associate-process-service-appln");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,kafkaParams));
        return stream.mapToPair(rec -> {
            String s = rec.value();
            System.out.println("record1 toString()..." + s);
            return new Tuple2<>(s, s);
        });
    }

}
