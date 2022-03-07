package main.java.org.poptweets;

import main.java.org.poptweets.bolt.HashTagFilterBolt;
import main.java.org.poptweets.bolt.LossyCountingBolt;
import main.java.org.poptweets.bolt.ReportBolt;
import main.java.org.poptweets.bolt.TweetSplitBolt;
import main.java.org.poptweets.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class PopTweetsTopology {
    private static final String TOPOLOGY_NAME = "popular-tweets-topology";
    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String TWEET_SPLIT_BOLT_ID = "tweet-split-bolt";
    private static final String HASH_TAG_FILTER_BOLT_ID = "hash-tag-filter-bolt";
    private static final String LOSSY_COUNTING_BOLT_ID = "lossy-counting-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";

    public static void main(String[] args) throws Exception {
        TwitterSpout spout = new TwitterSpout();
        TweetSplitBolt tweetSplitBolt = new TweetSplitBolt();
        HashTagFilterBolt hashTagFilterBolt = new HashTagFilterBolt();
        LossyCountingBolt lossyCountingBolt = new LossyCountingBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TWITTER_SPOUT_ID, spout);
        builder.setBolt(TWEET_SPLIT_BOLT_ID, tweetSplitBolt).shuffleGrouping(TWITTER_SPOUT_ID);
        builder.setBolt(HASH_TAG_FILTER_BOLT_ID, hashTagFilterBolt).fieldsGrouping(TWEET_SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(LOSSY_COUNTING_BOLT_ID, lossyCountingBolt).fieldsGrouping(HASH_TAG_FILTER_BOLT_ID, new Fields("hashTag"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(LOSSY_COUNTING_BOLT_ID);

        Config config = new Config();
        if (args.length == 3) {
            config.put("LOG_FILE_LOCATION", args[0]);
            config.put("EPSILON", Double.parseDouble(args[1]));
            config.put("THRESHOLD", Double.parseDouble(args[2]));
        }
        // TODO: Change the lines below to allow running on a Storm cluster
        // Use the line below to submit to Storm cluster
//        StormSubmitter.submitTopology(TOPOLOGY_NAME, config,builder.createTopology());

        // Use the section below to run locally
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Thread.sleep(1000 * 10);

        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
