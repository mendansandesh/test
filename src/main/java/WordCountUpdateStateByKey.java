package main.java;

import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.function.*;
//import com.google.common.base.Optional;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class WordCountUpdateStateByKey {
    public static void main(String[] args) throws Exception {

        if(args.length != 2 ){
            return;
        }
        else {
            Map<String, String> arguments = Splitter.on(",").withKeyValueSeparator("=").split(args[0]);

            SparkConf conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("AppSec Analytics")
                    .set("stream.hostname", arguments.get("stream.hostname"))
                    .set("stream.port", arguments.get("stream.port"));

            JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
            ssc.checkpoint("E:\\StreamingCheckpoints\\upadteStateByKey");
            JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK());
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
            JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<>(x, 1));


            Function2<List<Integer>, Optional<Integer>, Optional<Integer>>
                    updateFunction = (values, state) -> {
                Integer newSum = state.or(0);
                for (int i : values) {
                    newSum += i;
                }
                return Optional.of(newSum);
            };

            JavaPairDStream<String, Integer> runningCounts = pairs.updateStateByKey(updateFunction);


            runningCounts.print();
            ssc.start();              // Start the computation
            ssc.awaitTermination();
        }

    }
/*
    public static void startStateWithMap(JavaStreamingContext ssc){

        System.out.println("******************sandesh************************** " + ssc.sparkContext().getConf().get("stream.hostname"));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ssc.sparkContext().getConf().get("stream.hostname"),
                Integer.parseInt(ssc.sparkContext().getConf().get("stream.port")), StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> sublines = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
        JavaPairDStream<Integer, String> pairs = sublines.mapToPair((PairFunction<String, Integer, String>) s -> {
            String[] ar = s.split(",");
            return new Tuple2<>(Integer.parseInt(ar[0]), ar[1]);
        });

        final Function3<Integer, Optional<String>, State<String>, Tuple2<Integer, String>> mappingFunc =
                new Function3<Integer, Optional<String>, State<String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Integer id, Optional<String> name, State<String> state) {
                        String updateName = name.or("") + (state.exists() ? " " + state.get() : "");
                        Tuple2<Integer, String> output = new Tuple2<>(id, updateName);
                        state.update(updateName);
                        return output;
                    }
                };

        JavaPairDStream<Integer, String> stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc)).stateSnapshots();
        stateDstream.print();
    }
*/
}
