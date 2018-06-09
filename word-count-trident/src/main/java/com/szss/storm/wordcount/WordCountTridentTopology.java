package com.szss.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.StringTokenizer;

/**
 * @author 鼠笑天
 * @date 2017/12/27
 */
public class WordCountTridentTopology {

    public static class SplitFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            StringTokenizer st = new StringTokenizer(sentence);
            while (st.hasMoreElements()) {
                collector.emit(new Values(st.nextToken()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("This is my home"),
                new Values("That is your mother"),
                new Values("Where is my father"));
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("spout", spout)
                .each(new Fields("sentence"), new SplitFunction(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(3);
        StormTopology stormTopology = tridentTopology.build();
        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            // args[0]表示拓扑的名称
            StormSubmitter.submitTopology(args[0], config, stormTopology);
        } else {
            config.setMaxTaskParallelism(1);
            // 本地集群
            LocalCluster cluster = new LocalCluster();
            // 提交拓扑（该拓扑的名字叫word-count）
            cluster.submitTopology("word-count", config, stormTopology);
            Thread.sleep(5000);
            cluster.shutdown();
        }
    }
}
