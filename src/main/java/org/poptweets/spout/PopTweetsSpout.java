package main.java.org.poptweets.spout;

import main.java.org.poptweets.TwitterIntegration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;

import java.io.IOException;
import java.util.Map;

public class PopTweetsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        TwitterIntegration twitterIntegration = new TwitterIntegration();
        TwitterStream twitterStream = null;

        try {
            twitterStream = twitterIntegration.generateStream(this.collector);

            twitterStream.sample();
//        TimeUnit.SECONDS.sleep(10);
            Utils.sleep(1000 * 10);
            twitterStream.shutdown();

//            this.collector.emit(new Values());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}