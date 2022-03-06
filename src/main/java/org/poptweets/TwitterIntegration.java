package main.java.org.poptweets;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;

public class TwitterIntegration {

    public TwitterStream generateStream(SpoutOutputCollector collector) throws IOException, InterruptedException {
//        AuthProperties authValues = new AuthProperties();
//        HashMap configProperties = authValues.getPropertyValues();

        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                System.out.println(status.getText());
                collector.emit(new Values(status.getText()));
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
//        twitterStream.sample("en");


//        twitterStream.sample();
//
//        TimeUnit.SECONDS.sleep(10);
//
//        twitterStream.shutdown();

        return twitterStream;

    }
}
