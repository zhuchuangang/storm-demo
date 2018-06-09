package com.szss.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * @author 鼠笑天
 * @date 2017/12/30
 */
public class WordCountTridentFilterTopology {

    public static final void logFile(Object o, String log) {
        File file = new File("/root/storm.log");
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
            String processName = runtimeMXBean.getName();

            Thread thread = Thread.currentThread();
            String threadName = thread.getName();


            fileWriter.append("process:" + processName + " thread:" + threadName + " instance:" + o + " log:" + log + "\r\n");
            fileWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * a字段大于1的保留
     */
    public static class MyFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            int num = tuple.getInteger(0);
            logFile(this, "num:" + num);
            if (num > 1) {
                return true;
            }
            return false;
        }
    }

    /**
     * a b c 字段相加得到d
     */
    public static class MyFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            int a = tuple.getInteger(0);
            int b = tuple.getInteger(1);
            int c = tuple.getInteger(2);
            collector.emit(new Values(a + b + c));

        }
    }

    public static void main(String[] args) throws Exception {
        TridentTopology tridentTopology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b", "c"), 3,
                new Values(1, 2, 3),
                new Values(3, 2, 5),
                new Values(4, 6, 9),
                new Values(9, 2, 5));
        spout.setCycle(true);
        tridentTopology.newStream("spout", spout).parallelismHint(1)
                .shuffle().each(new Fields("a", "b", "c"), new MyFilter()).parallelismHint(6);
//                .shuffle().each(new Fields("a", "b", "c"), new MyFunction(), new Fields("d"))
//                .shuffle().partitionAggregate(new Fields("d"), new Sum(), new Fields("e"));


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
            Thread.sleep(3000);
            cluster.shutdown();
        }
    }
}
