package per.zzz;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 进行单词统计的任务
public class HelloCountWord {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取
        String path = "C:\\Users\\31961\\Desktop\\my\\Flink_study\\src\\main\\resources\\text.text";
        DataSource<String> dataSource = env.readTextFile(path);


        // 3.处理 单词按空格分开，转换成二元组（word,1）进行统计
        /**
         * 注意：Flink Java API不像Scala API可以随便写lambda表达式，写完以后需要使用returns方法显式指定返回值类型，否则会报错，
         *   错误提示意思就是说Java的lambda表达式不能提供足够的类型信息，需要指定返回值类型。不推荐使用lambda表达式而是使用匿名类。
         */
        AggregateOperator<Tuple2<String, Integer>> sum = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .groupBy(0) // 按照二元组第一个位置word进行分组
                .sum(1); // 对第二元组二个位置数据求和

        // 4.打印
        sum.print();


    }
}
