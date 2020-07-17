package main.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class NameChangeMapWithState {
    public static void main(String[] args) throws InterruptedException{
        Logger.getLogger(WordCountMapWithState.class).getLevel();
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("com").setLevel(Level.DEBUG);

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

        JavaDStream<String> sublines = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());

        JavaPairDStream<String, Employee> pairs = sublines.mapToPair((PairFunction<String, String, Employee>) s -> {
            String[] ar = s.split(",");
            Employee e = new Employee(ar[0], ar[1], ar[2]);
            return new Tuple2<>(e.getId(), e);
        });

        final Function3<String, Optional<Employee>, State<Employee>, Tuple2<String, Employee>> mappingFunc =
                new Function3<String, Optional<Employee>, State<Employee>, Tuple2<String, Employee>>() {
                    @Override
                    public Tuple2<String, Employee> call(String id, Optional<Employee> name, State<Employee> state) {
                        //String updateName = (state.exists() ? name.get().getName()  : name.get().getName());
                        //state.get().setName(updateName);
                        Employee eNew;
                        Tuple2<String, Employee> output;
                        if(state.exists()){
                            String updateName = name.get().getName();
                            eNew = state.get();
                            eNew.setName(updateName);
                            output = new Tuple2<>(id, eNew);
                            state.update(eNew);
                        }else{
                            eNew = name.get();
                            output = new Tuple2<>(id, eNew);
                            state.update(eNew);
                        }
                        return output;
                        /*Employee eNew = (state.exists() ? name.get() : name.get());
                        Tuple2<String, Employee> output = new Tuple2<>(id, eNew);
                        state.update(eNew);
                        return output; */
                    }
                    /*
                     @Override
                    public Tuple2<Integer, String> call(Integer id, Optional<String> name, State<String> state) {
                        //String updateName = name.or("") + (state.exists() ? " " + state.get() : "");
                        String updateName = state.exists() ? " " + name.or("") : name.or("");
                        Tuple2<Integer, String> output = new Tuple2<>(id, updateName);
                        state.update(updateName);
                        return output;
                    }*/
                };

        JavaPairDStream<String, Employee> stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc)).stateSnapshots();
        stateDstream.print();

        ssc.start();              // Start the computation
        ssc.awaitTermination();
    }
}
