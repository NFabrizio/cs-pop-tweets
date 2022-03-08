package main.java.org.poptweets.spout;

import main.java.org.poptweets.TwitterIntegration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import twitter4j.*;

import java.io.IOException;
import java.util.Map;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        // nextTuple() is called in an infinite loop in Apache Storm until the topology is killed
        System.out.println("************************ Executing TwitterSpout nextTuple ************************");
        TwitterIntegration twitterIntegration = new TwitterIntegration();
        TwitterStream twitterStream = null;

        try {
            twitterStream = twitterIntegration.generateStream(this.collector);

            // sample() gets data from the Twitter v1 API - https://stream.twitter.com/1.1/statuses/sample.json
            // "en" filters tweets that are classified as being written in English
            twitterStream.sample("en");

            // Sleep for 10 seconds to create a 10 second window
            Utils.sleep(1000 * 10);
            // Shutdown to avoid getting rate limited
            twitterStream.shutdown();
        } catch (IOException e) {
            System.out.println("TwitterSpout IOException");
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("TwitterSpout InterruptedException");
            e.printStackTrace();
        }

    }
}