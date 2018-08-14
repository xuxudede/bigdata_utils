import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/***
 * @Author: xushiyong
 * @Description
 * @Date: Created in 15:07 2018/6/28
 * @Modify By:
 **/
public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args){

        //定义SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local") ;
        //创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf) ;

        //读取文件创建RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/wc.txt");

        //RDD转换操作
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordcount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //执行Job
        wordcount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, Integer> wordcountTuple2) throws Exception {
                System.out.println("["+wordcountTuple2._1+","+wordcountTuple2._2+"]") ;
            }
        });

        sc.close();
    }
}
