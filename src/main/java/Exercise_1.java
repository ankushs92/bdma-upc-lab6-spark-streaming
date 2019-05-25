import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;

import java.time.LocalDateTime;

public class Exercise_1 {

    public static void displayAllTweets(JavaDStream<Status> tweets) {
        tweets
         .filter(status -> status.getLang().equalsIgnoreCase("en") || status.getLang().equalsIgnoreCase("english"))
         .map(status -> {
            String userName = status.getUser().getName();
            String tweet = status.getText();
            return "User with Username " + userName + ", Tweeted : " + tweet;
        }).print();
    }

}
