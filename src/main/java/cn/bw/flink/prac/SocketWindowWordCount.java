package cn.bw.flink.prac;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        Host host = new Host();
        ParameterTool params;
        try {
            params = ParameterTool.fromArgs(args);
            host.setHostname(params.has("hostname") ? params.get("hostname") : "localhost");
            host.setPort(params.getInt("port"));
        } catch (Exception e) {
            System.err.println("未指定端口." +
                    "请执行'SocketWindowWordCount --hostname <hostname> --port <port>'," +
                    "主机名(默认localhost), " +
                    "端口是文本服务器的地址");
            System.err.println("请执行'netcat -l <port>'命令, 启动一个简单的文本服务器, 然后在命令行输入文本");
            return;
        }

        // 准备运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取输入的文本
        DataStream<String> text = env.socketTextStream(host.getHostname(), host.getPort(), "\n");

        // 执行解析数据, 分组, 窗口化, 聚合操作
        // 每5秒打印一次单词出现的次数
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        Arrays.asList(value.split("\\s")).forEach(word -> out.collect(new WordWithCount(word, 1L)));
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce((ReduceFunction<WordWithCount>) (a, b) -> new WordWithCount(a.getWord(), a.getCount() + b.getCount()));
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }




}
