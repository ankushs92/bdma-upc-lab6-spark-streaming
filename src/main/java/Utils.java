import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {

    static {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);
    }

    static void setupTwitter(String path) throws Exception {
        Properties props = new Properties();
        props.load(new FileReader(path));

        String consumerKey = props.getProperty("consumerKey");
        String consumerSecret = props.getProperty("consumerSecret");
        String accessToken = props.getProperty("accessToken");
        String accessTokenSecret = props.getProperty("accessTokenSecret");

        configureTwitterCredentials(consumerKey, consumerSecret, accessToken,
                accessTokenSecret);
    }

    static void configureTwitterCredentials(String apiKey, String apiSecret,
                                            String accessToken, String accessTokenSecret) throws Exception {
        HashMap<String, String> configs = new HashMap<String, String>();
        configs.put("apiKey", apiKey);
        configs.put("apiSecret", apiSecret);
        configs.put("accessToken", accessToken);
        configs.put("accessTokenSecret", accessTokenSecret);
        Object[] keys = configs.keySet().toArray();
        for (int k = 0; k < keys.length; k++) {
            String key = keys[k].toString();
            String value = configs.get(key).trim();
            if (value.isEmpty()) {
                throw new Exception("Error setting authentication - value for "
                        + key + " not set");
            }
            String fullKey = "twitter4j.oauth."
                    + key.replace("api", "consumer");
            System.setProperty(fullKey, value);
            System.out.println("\tProperty " + key + " set as [" + value + "]");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        List<String> list = Arrays.asList("Hello", " my", " name ", " is ", " Ankush");
        String result = "";
        for(String l : list) {
            result+= l;
        }
        System.out.println(result);


        String name = String.join("", list);
        System.out.println(name);


        String complicated = list.stream().collect(Collectors.joining(""));
        System.out.println(complicated);
    }

}