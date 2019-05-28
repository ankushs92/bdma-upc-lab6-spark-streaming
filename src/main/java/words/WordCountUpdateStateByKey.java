package words;

import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.Arrays;

public class WordCountUpdateStateByKey {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "");
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(5000));
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();
        jsc.checkpoint(Files.createTempDir().getAbsolutePath());
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);

        lines.flatMap(string -> {
            System.out.println("Message arrived at " + LocalDateTime.now());
            System.out.println(string);
            String[] array = string.split(" ");
            return Arrays.asList(array).iterator();
        }).mapToPair(word -> {
            return new Tuple2<>(word, 1);
        })
        .updateStateByKey((valuesList, currentState) -> {
            System.out.println("Current List for Key" + valuesList);
            System.out.println(currentState);
            int currentValue = 0;
            if(!currentState.isPresent()) {
                currentValue =  currentValue + valuesList.stream().reduce(0, (x, y) ->  x + y);
            }
            else {
                currentValue += (int) currentState.get() + valuesList.stream().reduce(0, (x, y) ->  x + y);
            }
            return Optional.ofNullable(currentValue);
        })
        .transformToPair(rdd -> {
            return rdd
                    .mapToPair(tuple -> tuple.swap())
                    .sortByKey(false);
        })
        .foreachRDD(rdd -> {
            System.out.println(rdd.take(10));
        });

        jsc.start();          // Start the computation
        jsc.awaitTermination();  // Wait for the computation to terminate
    }
}
