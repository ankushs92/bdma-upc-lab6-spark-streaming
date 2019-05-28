package words;

import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.Arrays;

public class WordCountReduceByKeyAndWindow {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "");
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(1000));
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();
        jsc.checkpoint(Files.createTempDir().getAbsolutePath());
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);

        lines.flatMap(string -> {
            System.out.println(string);
            String[] array = string.split(" ");
            return Arrays.asList(array).iterator();
        }).mapToPair(word -> {
            System.out.println("Word is " + word);
            return new Tuple2<>(word, 1);
        }).reduceByKeyAndWindow(
                (accum, count2) -> {
                    System.out.println("----------");
                    System.out.println(LocalDateTime.now());
                    System.out.println("Accum in reduce function is " + accum);
                    System.out.println("Count in reduce function is  is " + count2);
                    System.out.println("----------");

                    return accum + count2;
                 },
                (accum, count) -> {
                    System.out.println("----------");
                    System.out.println(LocalDateTime.now());
                    System.out.println("Accum in inverse function is " + accum);
                    System.out.println("Count in inverse function is  is " + count);
                    System.out.println("----------");
                    return accum - count;
                },
                new Duration(10000),
                new Duration(5000)

        ).transformToPair(rdd -> {
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
