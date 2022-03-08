package main.java.org.poptweets;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;

public class TwitterIntegration {

    public TwitterStream generateStream(SpoutOutputCollector collector) throws IOException, InterruptedException {
        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                // When a status is received, emit its value to make it available to the next part of the chain
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

        // Create a new Twitter stream and listen to it
        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();
        twitterStream.addListener(listener);

        return twitterStream;

    }
}
