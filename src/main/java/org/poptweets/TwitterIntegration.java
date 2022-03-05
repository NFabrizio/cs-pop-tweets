package main.java.org.poptweets;

import main.java.org.poptweets.utils.AuthProperties;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.HashMap;

public class TwitterIntegration {

    public static void main(String[] args) throws IOException {
        AuthProperties authValues = new AuthProperties();
        HashMap configProperties = authValues.getPropertyValues();

        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                System.out.println(status.getText());
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long l, long l1) {}

            @Override
            public void onStallWarning(StallWarning stallWarning) {}

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();
        twitterStream.addListener(listener);
        // Add a twitter4j.properties file to the classpath when running program, and the lines below will not be needed
//        twitterStream.setOAuthConsumer((String) configProperties.get("oauth.consumerKey"), (String) configProperties.get("oauth.consumerSecret"));
//        AccessToken token =
//                new AccessToken((String) configProperties.get("oauth.accessToken"), (String) configProperties.get("oauth.accessTokenSecret"));
//        twitterStream.setOAuthAccessToken(token);
        twitterStream.sample("en");
    }
}
