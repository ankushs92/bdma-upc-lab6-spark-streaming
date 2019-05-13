import org.apache.spark.streaming.api.java.JavaDStream;
import twitter4j.Status;

public class Exercise_2 {

    public static void get10MostPopularHashtagsInLast5min(JavaDStream<Status> statuses) {

        /**
         *  Get the stream of hashtags from the stream of tweets
         */
		/*
		JavaDStream<String> words = statuses
				.flatMap();

		JavaDStream<String> hashTags = words
				.filter();
		*/
        //hashTags.print();

        /**
         *  Count the hashtags over a 5 minute window
         */
        /*
		JavaPairDStream<String, Integer> tuples = hashTags.
				mapToPair();

		JavaPairDStream<String, Integer> counts = tuples.
				reduceByKeyAndWindow();
		*/
        //counts.print();

        /**
         *  Find the top 10 hashtags based on their counts
         */
		/*
		JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair();

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair();

		sortedCounts.foreachRDD();
		*/
    }

}