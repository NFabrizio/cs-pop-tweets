package main.java.org.poptweets;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LossyCounting extends BaseRichBolt {
    private OutputCollector collector;
    private final Map<String, Objects> bucket = new ConcurrentHashMap<String, Objects>();
    private double eps;
    private final double t;
    private int element = 0;
    private int usedBucket = 1;
    private final int size = (int)Math.ceil(1 / eps);
    private long initTime, nowTime;

    public LossyCounting(double eps, double t) {
        this.eps = eps;
        this.t = t;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        initTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple){
        String content;
        //content = tuple.toString();
        content = tuple.getStringByField("hashTag");
        if (element < size) {
            if (!bucket.containsKey(content)) {
                Objects d = new Objects();
                d.delta = usedBucket - 1;
                d.count = 1;
                d.element = content;
                bucket.put(content, d);
            } else {
                Objects d = bucket.get(content);
                d.count += 1;
                bucket.put(content, d);
            }
            element += 1;
        }

        nowTime = System.currentTimeMillis();
        if (nowTime >= initTime + 10000) {
            if (!bucket.isEmpty()) {
                HashMap<String, Integer> tempOrdering = new HashMap<String, Integer>();
                for (String keySet : bucket.keySet()) {
                    Objects objkeySet= bucket.get(keySet);
                    double a = (t - eps) * element;
                    boolean sign = objkeySet.count >= a;
                    if (sign) {
                        objkeySet.threshold = (float)objkeySet.svalue / objkeySet.count;
                        tempOrdering.put(keySet, objkeySet.count);
                    }
                }
                if (!tempOrdering.isEmpty()) {
                    List<String> keys = new ArrayList<String>(tempOrdering.keySet());
                    List<Integer> values = new ArrayList<Integer>(tempOrdering.values());
                    keys.sort(Collections.reverseOrder());
                    values.sort(Collections.reverseOrder());

                    LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

                    for (Integer tempValue : values) {
                        Iterator<String> keyIterator = keys.iterator();

                        while (keyIterator.hasNext()) {
                            String key = keyIterator.next();
                            Integer temp = tempOrdering.get(key);

                            if (temp.equals(tempValue)) {
                                keyIterator.remove();
                                sortedMap.put(key, tempValue);
                                break;
                            }
                        }
                    }
                    Collection<String> str;
                    if(sortedMap.size()>100){
                        Collections.list(Collections.enumeration(sortedMap.keySet())).subList(0, 100);
                    }
                    str = sortedMap.keySet();
                    LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
                    for(String key: str){
                        finalEmit.put("<"+key+":"+bucket.get(key).threshold+">", bucket.get(key).count);
                    }
                    collector.emit(new Values(finalEmit.keySet().toString(), nowTime));
                    //System.out.println("str:" + str.toString());
                }
            }
            initTime = nowTime;

        }
        if (size == element) {
            for (String word : bucket.keySet()) {
                Objects d = bucket.get(word);
                double sum = d.count + d.delta;
                if (sum <= usedBucket) {
                    bucket.remove(word);
                }
            }
            element = 0;
            usedBucket += 1;
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tag", "time"));
    }
}
