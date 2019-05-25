import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Exercise_2 {

    public static void get10MostPopularHashtagsInLast5min(JavaDStream<Status> statuses) {
        statuses
                .filter(status -> status.getText().contains("#"))
                .flatMap(status ->  Arrays.asList(status.getText().split(" ")).iterator())
                .filter(string -> string.contains("#"))
                .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
                .reduceByKeyAndWindow(
                        (accum, value) -> {
                            return accum + value;
                        },
                        (x, y) -> {
                            return x - y;
                        },
                        new Duration(10000),
                        new Duration(2000)
                )
                .mapToPair(tuple -> tuple.swap())
                .transformToPair(rdd -> {
                    return rdd.sortByKey(false);
                })
                .foreachRDD(rdd -> {
                    System.out.println("==============================================================================================================");
                    System.out.println(LocalDateTime.now());
                    System.out.println(rdd.take(10));
                    System.out.println("==============================================================================================================");
                });
        
    }

}