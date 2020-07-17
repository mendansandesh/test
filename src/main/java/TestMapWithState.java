package main.java;

import com.google.common.base.Splitter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import scala.collection.immutable.Nil;


import java.util.*;
import java.util.regex.Pattern;

public class TestMapWithState {
    public static void main(String[] args) throws Exception {
        Logger.getLogger(WordCountMapWithState.class).getLevel();
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("com").setLevel(Level.DEBUG);

        /*
        SparkConf conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("WordCountMapWithState")
                    .set("stream.hostname", "localhost");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        */

/*        if(args.length != 1 ){
            return;
        }
        else {
*/         // Map<String, String> arguments = Splitter.on(" ").withKeyValueSeparator("=").split(args[0]);
/*            Map<String, String> m = new HashMap<>();
            List<String> m1 = new ArrayList<>();
            m1.add("couchbase.ipPortList");
            m1.add("couchbase.userId");
            m1.add("couchbase.password");
            m1.add("kafka.bootstrap.servers");
            for(int i=0; i< args.length; i++){
                if (args[i].split("=").length != 2) {
                    System.out.println("Incorrect Arguments: Please check the argument list supplied, as one or more argument value could be missing");
                    return;
                }
                if(!m1.contains("args[i].split(\"=\")[0]")){
                    System.out.println(args[i].split("=")[0] +" argument is not a valid argument");
                    return;
                }
                m.put(args[i].split("=")[0], args[i].split("=")[1]);
            }
            System.out.println("*******kafka brokers*******" + m.get("kafka.bootstrap.servers"));
            List<String> brokersList = new ArrayList<>();
            for(int i=0; i< m.get("kafka.bootstrap.servers").split(",").length; i++){
                brokersList.add(m.get("kafka.bootstrap.servers").split(",")[i]);
                System.out.println("*******kafka brokers*******" + brokersList.get(i));
            }

            SparkConf conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("AppSec Analytics")
                    .set("stream.hostname", m.get("stream.hostname"))
                    .set("stream.port", m.get("stream.port"));
*/
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("AppSec Analytics")
                .set("stream.hostname", args[0])
                .set("stream.port", args[1]);
            SparkSession spark = SparkSession
                    .builder()
                    .config(conf)
                    .getOrCreate();

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

            ssc.checkpoint("E:\\StreamingCheckpoints\\mapWithState");


        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ssc.sparkContext().getConf().get("stream.hostname"),
                Integer.parseInt(ssc.sparkContext().getConf().get("stream.port")), StorageLevel.MEMORY_AND_DISK());
/*
        JavaDStream<String> subquery = lines.flatMap(x -> Arrays.asList(Pattern.compile(",").split(x)).iterator());
        JavaPairDStream<String, Integer> subqueryPair = subquery.mapToPair((PairFunction<String, String, Integer>) s -> {
           String[] str = s.split(" ");
           return new Tuple2<>(str[0], 1);
        });

        final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>(){
                    @Override
                    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
                        int sum = one.or(0) + (state.exists() ? state.get() : 0);
                        Tuple2<String, Integer> output = new Tuple2<String, Integer>(word, sum);
                        state.update(sum);
                        return output;
                    }
                };

        JavaPairDStream<String, Integer> subqueryPairCount = subqueryPair.mapWithState(StateSpec.function(mappingFunc)).stateSnapshots();
        subqueryPairCount.print();
*/

            //simply testing by having outside this class
            //WordCountUpdateStateByKey.startStateWithMap(ssc);

        JavaDStream<String> sublines = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
        JavaPairDStream<Integer, String> pairs = sublines.mapToPair((PairFunction<String, Integer, String>) s -> {
            String[] ar = s.split(",");
            return new Tuple2<>(Integer.parseInt(ar[0]), ar[1]);
        });

        final Function3<Integer, Optional<String>, State<String>, Tuple2<Integer, String>> mappingFunc =
                new Function3<Integer, Optional<String>, State<String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Integer id, Optional<String> name, State<String> state) {
                        //String updateName = name.or("") + (state.exists() ? " " + state.get() : "");
                        String updateName = state.exists() ? " " + name.or("") : name.or("");
                        Tuple2<Integer, String> output = new Tuple2<>(id, updateName);
                        state.update(updateName);
                        return output;
                    }
                };

        JavaPairDStream<Integer, String> stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc)).stateSnapshots();
        stateDstream.print();

            ssc.start();
            ssc.awaitTermination();
    }
}
