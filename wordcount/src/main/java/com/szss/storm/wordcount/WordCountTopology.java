package com.szss.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * @author 鼠笑天
 * @date 2017/12/23
 */
public class WordCountTopology {

    /**
     * 定义一个spout，用于随机产生数据（从字符串数组中随机取出字符串），并继承BaseRichSpout，或者是实现IRichSpout接口
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        private String[] sentences;
        private SpoutOutputCollector collector;
        private Random random;

        public RandomSentenceSpout() {
            sentences = new String[]{"This is my home", "That is your mother", "Where is my father"};
            random = new Random();
        }

        /**
         * 当 一个task被初始化时会调用open方法，一般都会在此方法中初始化发送tuple的对象SpoutOutputCollector和配置对象TopologyContext.
         *
         * @param map
         * @param topologyContext
         * @param spoutOutputCollector
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            collector = spoutOutputCollector;
        }

        /**
         * 这是Spout类中最重要的一个方法，发射一个Tuple到Topology都是通过该方法来实现的。
         */
        @Override
        public void nextTuple() {
            String sentence = sentences[random.nextInt(sentences.length)];
            collector.emit(new Values(sentence));
        }

        /**
         * 此方法用于声明当前Spout的Tuple发送流。流的定义是通过OutputFieldsDeclare.declareStream方法完成的，其中的参数包括了发送的域Fields
         *
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }
    }

    /**
     * 用于将句子切分为单词
     */
    public static class SplitBolt extends BaseBasicBolt {
        /**
         * 拆分单词
         *
         * @param tuple
         * @param basicOutputCollector
         */
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            //该bolt的上游的spout只输出一个字段
            String sentence = tuple.getString(0);
            //根据空格进行拆分
            StringTokenizer st = new StringTokenizer(sentence);
            while (st.hasMoreElements()) {
                basicOutputCollector.emit(new Values(st.nextToken()));
            }

        }

        /**
         * 输出字段名称设置为word
         *
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }


    /**
     * 用于单词计数
     */
    public static class WordCountBolt extends BaseBasicBolt {

        private Map<String, Integer> count;

        public WordCountBolt() {
            this.count = new HashMap(10);
        }

        /**
         * 统计单词数量
         *
         * @param tuple
         * @param basicOutputCollector
         */
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            //获取上游SplitBolt发出的字段word
            String word = tuple.getString(0);
            Integer c = count.get(word);
            if (c != null) {
                c++;
            } else {
                c = 1;
            }
            count.put(word, c);
            //basicOutputCollector.emit(new Values(word, c));
        }

        /**
         * 定义输出字段
         *
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));
        }


        @Override
        public void cleanup() {
            for (String key : count.keySet()) {
                System.out.println("==============" + key + ":" + count.get(key));
            }

        }
    }


    public static void main(String[] args) throws Exception {
        // 创建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置Spout，这个Spout的名字叫做"spout"，设置并行度为1
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        // 设置slot——“split”，并行度为1，它的数据来源是spout的
        builder.setBolt("split", new SplitBolt(), 2).shuffleGrouping("spout");
        // 设置slot——“count”,你并行度为1，它的数据来源是split的word字段
        builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("split", new Fields("word"));
        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            // args[0]表示拓扑的名称
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(1);
            // 本地集群
            LocalCluster cluster = new LocalCluster();
            // 提交拓扑（该拓扑的名字叫word-count）
            cluster.submitTopology("word-count", config, builder.createTopology());
            Thread.sleep(3000);
            cluster.shutdown();
        }
    }
}

