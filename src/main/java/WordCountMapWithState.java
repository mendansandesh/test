package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WordCountMapWithState {
    public static void main(String[] args) throws Exception {
        Logger.getLogger(WordCountMapWithState.class).getLevel();
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("com").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCountMapWithState");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        ssc.checkpoint("E:\\StreamingCheckpoints\\mapWithState");
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<>(x, 1));


        final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
                        int sum = one.or(0) + (state.exists() ? state.get() : 0);
                        Tuple2<String, Integer> output = new Tuple2<String, Integer>(word, sum);
                        state.update(sum);
                        return output;
                    }
                };

        JavaPairDStream<String, Integer> stateDstream =
                pairs.mapWithState(StateSpec.function(mappingFunc)).stateSnapshots();

        stateDstream.print();
        ssc.start();              // Start the computation
        ssc.awaitTermination();
    }

}
