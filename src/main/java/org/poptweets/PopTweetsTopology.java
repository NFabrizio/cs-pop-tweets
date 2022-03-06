package main.java.org.poptweets;

import main.java.org.poptweets.bolt.ReportBolt;
import main.java.org.poptweets.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class PopTweetsTopology {
    private static final String TOPOLOGY_NAME = "popular-tweets-topology";
    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String REPORT_BOLT_ID = "report-bolt";

    public static void main(String[] args) throws Exception {
        TwitterSpout spout = new TwitterSpout();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TWITTER_SPOUT_ID, spout);
        builder.setBolt(REPORT_BOLT_ID, reportBolt).shuffleGrouping(TWITTER_SPOUT_ID);

        Config config = new Config();
        if (args.length == 1) {
//            config.put("LOG_FILE_LOCATION", "abc123");
            config.put("LOG_FILE_LOCATION", args[0]);
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
