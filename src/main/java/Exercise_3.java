import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

public class Exercise_3 {

    public static void historicalAnalysis(JavaDStream<Status> statuses) {
          JavaPairDStream<Object, String> commonStream = statuses
                                                            .filter(status -> (status.getLang().equalsIgnoreCase("en") || status.getLang().equalsIgnoreCase("english")))
                                                            .flatMap(status ->  Arrays.asList(status.getText().split(" ")).iterator())
                                                            .filter(string -> string.contains("#"))
                                                            .mapToPair(hashTag -> {
                                                                return new Tuple2<>(hashTag, 1);
                                                            })
                                                            .updateStateByKey((valuesList, optionalCurrentState) -> {
                                                                int currentValue = 0;
                                                                if(!optionalCurrentState.isPresent()) {
                                                                    currentValue = valuesList.stream().reduce(0, (x, y) -> x + y);
                                                                }
                                                                else {
                                                                    currentValue += valuesList.stream().reduce(0, (x, y) -> x + y) + (int) optionalCurrentState.get();
                                                                }
                                                                return Optional.of(currentValue);
                                                            })
                                                            .transformToPair(rdd -> {
                                                                return rdd
                                                                        .mapToPair(tuple -> tuple.swap())
                                                                        .sortByKey(false);
                                                            }).cache();


        //Median hashtag

        //For top 10
        commonStream.foreachRDD( rdd -> {
            rdd.take(10).forEach(element -> {
                   System.out.println(element.swap());
            });
        });
//                                                            .foreachRDD(rdd -> {
//                                                                System.out.println("============");
//                                                                rdd.take(1).forEach(element -> {
//                                                                    System.out.println(element.swap());
//                                                                });
//
//                                                                System.out.println("============");
//
//                                                            });






    }


}
