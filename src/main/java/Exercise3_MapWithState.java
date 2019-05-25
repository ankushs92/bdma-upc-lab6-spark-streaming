import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;

public class Exercise3_MapWithState {

    @SuppressWarnings("Duplicates")
    public static void historicalAnalysis(JavaDStream<Status> statuses) {

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Object>> mappingFunction = (key, optionalCurrentValue, state) -> {
            int currentValue = 1;
//            System.out.println("Key = " + key + ", state " + state + " , optional current value " + optionalCurrentValue);
            if(state.exists()) {
                currentValue += state.get();
            }
            state.update(currentValue);
            return new Tuple2<>(key, currentValue);
        };
        statuses
                .filter(status -> status.getText().contains("#") && (status.getLang().equalsIgnoreCase("en") || status.getLang().equalsIgnoreCase("english")))
                .flatMap(status ->  Arrays.asList(status.getText().split(" ")).iterator())
                .filter(string -> string.contains("#"))
                .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
                .mapWithState(StateSpec.function(mappingFunction))
                .transformToPair(rdd ->
                        rdd
                        .mapToPair(tuple -> tuple.swap())
                        .sortByKey(false)
                )
                .foreachRDD(rdd -> {
//                    System.out.println("============");
                    List<Tuple2<Object, String>> list = rdd.take(1);
                    list.forEach(element -> {
                        System.out.println(element.swap());
                    });

//                    System.out.println("============");
                });


    }

    public static void main(String[] args) {

    }
}
