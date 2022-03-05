package main.java.org.poptweets.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class AuthProperties {
    public HashMap getPropertyValues() throws IOException {
        HashMap<String, String> configProps = new HashMap<String, String>();
        InputStream inputStream = null;

        try {
            Properties props = new Properties();
            String propFile = "twitter4j.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFile);

            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException(propFile + " not found in classpath");
            }

            configProps.put("oauth.consumerKey", props.getProperty("oauth.consumerKey"));
            configProps.put("oauth.consumerSecret", props.getProperty("oauth.consumerSecret"));
            configProps.put("oauth.accessToken", props.getProperty("oauth.accessToken"));
            configProps.put("oauth.accessTokenSecret", props.getProperty("oauth.accessTokenSecret"));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }

        return configProps;
    }
}
