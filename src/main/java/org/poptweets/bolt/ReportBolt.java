package main.java.org.poptweets.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ReportBolt extends BaseRichBolt {
//    private HashMap<String, Long> counts = null;
    private ArrayList<String> hashTags = null;
    private String logPath = "TwitterSpoutLog.txt";

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
//        this.counts = new HashMap<String, Long>();
        String pathArg = (String) config.get("LOG_FILE_LOCATION");
        if(pathArg != null && !pathArg.trim().isEmpty()) {
            this.logPath = pathArg;
        }
        this.hashTags = new ArrayList<String>();
    }

    public void execute(Tuple tuple) {
//        String word = tuple.getStringByField("word");
//        Long count = tuple.getLongByField("count");
//        this.counts.put(word, count);
        String hashTag = tuple.getStringByField("hashTag");
        this.hashTags.add(hashTag);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
//        System.out.println("--- FINAL COUNTS ---");
//        List<String> keys = new ArrayList<String>();
//        keys.addAll(this.counts.keySet());
//        Collections.sort(keys);
//        for (String key : keys) {
//            System.out.println(key + " : " + this.counts.get(key));
//        }
//        System.out.println("--- FINAL TWEETS ---");
//        for (String tweet : tweets) {
//            System.out.println(tweet);
//        }

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(this.logPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        };
        BufferedWriter bw = new BufferedWriter(fileWriter);
        for (String hashTag : hashTags){
            System.out.println(hashTag);
            try {
                bw.write(hashTag);
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
