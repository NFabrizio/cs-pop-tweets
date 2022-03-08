package main.java.org.poptweets.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ReportBolt extends BaseRichBolt {
    private ArrayList<String> hashTags = null;
    private LinkedHashMap<String, Integer> hashTagsList = null;
    // Default log file path if none is provided
    private String logPath = "TwitterSpoutLog.txt";
    private String time;
    private ArrayList<String> counts = null;
    private long startTime, nowTime;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.startTime = System.currentTimeMillis();
        String pathArg = (String) config.get("LOG_FILE_LOCATION");

        if (pathArg != null && !pathArg.trim().isEmpty()) {
            this.logPath = pathArg;
        }

        this.hashTags = new ArrayList<String>();
        hashTagsList = new LinkedHashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        // Ex. "[hashtag1=3, hashtag2=1, hashtag3=1]"
        String hashTagList = tuple.getStringByField("tags");
        // Ex. "1646759511696"
        this.time = String.valueOf(tuple.getLongByField("time"));

        // Tags come in as a list surrounded by [], substring to remove these
        hashTagList = hashTagList.substring(1, hashTagList.length() - 1);
            String[] tags = hashTagList.split(", ");

            // Split each tag pair (e.g., hashtag1=3) and add them as keys and values to the map
            for (String tagPair : tags) {
                String[] tagKeyValue = tagPair.trim().split("=");
                Boolean valueExists = hashTagsList.containsKey(tagKeyValue[0]);

                // If the key already exists, aggregate the values otherwise add the key and value
                if (valueExists) {
                    hashTagsList.put(tagKeyValue[0], Integer.parseInt(tagKeyValue[1]) + hashTagsList.get(tagKeyValue[0]));
                } else {
                    hashTagsList.put(tagKeyValue[0], Integer.parseInt(tagKeyValue[1]));
                }
            }

        nowTime = System.currentTimeMillis();

            // Every 10 seconds, write to log file and reset persisted data
        if (nowTime >= this.startTime + 10000) {
            System.out.println("************************ Logging from ReportBolt ************************");
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(this.logPath, true);
            } catch (IOException e) {
                System.out.println("************************ ReportBolt filewriter IOException ************************");
                e.printStackTrace();
            }

            BufferedWriter bw = new BufferedWriter(fileWriter);
            try {
                // Sort linked hash map by copying to ArrayList and sorting in descending order
                List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(this.hashTagsList.entrySet());
                Collections.sort(entries, Collections.reverseOrder(new Comparator<Map.Entry<String, Integer>>() {
                    public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b){
                        return a.getValue().compareTo(b.getValue());
                    }
                }));

                Map<String, Integer> sortedEntries = new LinkedHashMap<String, Integer>();

                // Convert sorted ArrayList back to map for easier display
                for (Map.Entry<String, Integer> entry : entries) {
                    sortedEntries.put(entry.getKey(), entry.getValue());
                }

                // Ex. "1646758699746 [#BTSARMY, #PS4, #ETH, #SB19]"
                bw.write(this.time + " ");
                bw.write(sortedEntries.keySet().toString());
                bw.newLine();
            } catch (IOException e) {
                System.out.println("ReportBolt bw.write IOException Error occurred while attempting to write logs");
                e.printStackTrace();
            }

            try {
                bw.flush();
            } catch (IOException e) {
                System.out.println("************************ ReportBolt flush IOException ************************");
                e.printStackTrace();
            }

            this.hashTags = new ArrayList<String>();
            this.hashTagsList = new LinkedHashMap<String, Integer>();

            this.startTime = nowTime;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
        // Since this program is designed to run until stopped manually, this method should never be called
        System.out.println("************************ Cleaning up ReportBolt ************************");

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(this.logPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ;

        BufferedWriter bw = new BufferedWriter(fileWriter);

        try {
            Utils.sleep(1000 * 10);

            bw.write(this.time + " ");
            System.out.println("hashTags.size()");
            System.out.println(this.hashTags.size());

            int index = 0;
            int longestIndex = 0;
            for (String tagList : this.hashTags) {
                String[] tags = tagList.split(",");
                if (tags.length > longestIndex) {
                    longestIndex = index;
                }
                index++;
            }

            bw.write(this.hashTags.get(longestIndex));
            bw.newLine();
        } catch (IOException e) {
            System.out.println("Error occurred while attempting to write logs");
            e.printStackTrace();
        }

        for (String hashTag : hashTags) {
            System.out.println(hashTag);
        }

        try {
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Utils.sleep(1000 * 10);
    }
}
