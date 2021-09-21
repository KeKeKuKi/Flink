package per.zzz;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 进行单词统计的任务
public class StreamCountWord {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从本地文件 读取数据
//        String path = "C:\\Users\\31961\\Desktop\\my\\Flink_study\\src\\main\\resources\\text.text";
//        DataStreamSource<String> streamSource = env.readTextFile(path);

        // 从服务器文本流 读取 socket 数据
        // yum install netcat 下载NC
        // nc -lk 7777 启动监听
        DataStreamSource<String> streamSource = env.socketTextStream("121.37.67.53", 7777);


        // 3.基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        // 4.输出 注：输出x> x代表分区编号
        sum.print();

        // 5.启动任务
        env.execute();

    }
}
