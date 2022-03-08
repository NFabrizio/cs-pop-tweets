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
        int parallelism = 1;

        if (args.length == 4) {
            parallelism = Integer.parseInt(args[3]);
        }
        TwitterSpout spout = new TwitterSpout();
        TweetSplitBolt tweetSplitBolt = new TweetSplitBolt();
        HashTagFilterBolt hashTagFilterBolt = new HashTagFilterBolt();
        LossyCountingBolt lossyCountingBolt = new LossyCountingBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TWITTER_SPOUT_ID, spout);
        builder.setBolt(TWEET_SPLIT_BOLT_ID, tweetSplitBolt).shuffleGrouping(TWITTER_SPOUT_ID);
        builder.setBolt(HASH_TAG_FILTER_BOLT_ID, hashTagFilterBolt).fieldsGrouping(TWEET_SPLIT_BOLT_ID, new Fields("word"));
        // Use parallelism to configure number of executors per worker
        builder.setBolt(LOSSY_COUNTING_BOLT_ID, lossyCountingBolt, parallelism).fieldsGrouping(HASH_TAG_FILTER_BOLT_ID, new Fields("hashTag"));
        // Use global grouping so that all results from lossy counting are sent to a single report bolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(LOSSY_COUNTING_BOLT_ID);

        Config config = new Config();
        // Use parallelism to configure number of worker nodes
        config.setNumWorkers(parallelism);

        if (args.length > 2) {
            // Add values to config so that they are available for bolts
            config.put("LOG_FILE_LOCATION", args[0]);
            config.put("EPSILON", Double.parseDouble(args[1]));
            config.put("THRESHOLD", Double.parseDouble(args[2]));
        }

        // Use the line below rather than LocalCluster to be able to submit to Storm cluster
        StormSubmitter.submitTopology(TOPOLOGY_NAME, config,builder.createTopology());

        // Use the section below rather than StormSubmitter to run locally
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }
}
